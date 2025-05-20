#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

namespace DB::Parquet
{

bool ShutdownHelper::try_lock_shared()
{
    Int64 n = val.fetch_add(1, std::memory_order_acquire) + 1;
    chassert(n != SHUTDOWN_START);
    if (n >= SHUTDOWN_START)
    {
        unlock_shared();
        return false;
    }
    return true;
}

void ShutdownHelper::unlock_shared()
{
    Int64 n = val.fetch_sub(1, std::memory_order_release) - 1;
    chassert(n >= 0);
    if (n == SHUTDOWN_START)
    {
        /// We're the last completed task. Add SHUTDOWN_END to indicate that no further waiting
        /// or cv notifying is needed, even though `val` can get briefly bumped up and down by
        /// unsuccessful try_lock_shared() calls.
        val.fetch_add(SHUTDOWN_END);
        {
            /// Lock and unlock the mutex. This may look weird, but this is usually (always?)
            /// required to avoid race conditions when combining condition_variable with atomics.
            ///
            /// In this case, the prevented race condition is:
            ///  1. unlock_shared() sees n == SHUTDOWN_START,
            ///  2. shutdown thread enters cv.wait(lock, [&] { return val.load() >= SHUTDOWN_END; });
            ///     the callback does val.load(), gets SHUTDOWN_START, and is about
            ///     to return false; at this point, the cv.wait call is not monitoring
            ///     condition_variable notifications (remember that cv.wait with callback is
            ///     equivalent to a wait without callback in a loop),
            ///  3. the unlock_shared() assigns `val` and calls cv.notify_all(), which does
            ///     nothing because no thread is blocked on the condition variable,
            ///  4. the cv.wait callback returns false; the wait goes back to sleep and never
            ///     wakes up.
            std::unique_lock lock(mutex);
        }
        cv.notify_all();
    }
}

bool ShutdownHelper::shutdown_requested()
{
    return val.load(std::memory_order_relaxed) >= SHUTDOWN_START;
}

void ShutdownHelper::begin_shutdown()
{
    Int64 n = val.fetch_add(SHUTDOWN_START) + SHUTDOWN_START;
    chassert(n < SHUTDOWN_START * 2); // shutdown requested more than once
    if (n == SHUTDOWN_START)
        val.fetch_add(SHUTDOWN_END);
}

void ShutdownHelper::wait_shutdown()
{
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return val.load() >= SHUTDOWN_END; });
}

void ShutdownHelper::shutdown()
{
    begin_shutdown();
    wait_shutdown();
}


#ifdef OS_LINUX

bool CompletionNotification::check() const
{
    return val.load(std::memory_order_acquire) == NOTIFIED;
}

void CompletionNotification::wait()
{
    UInt32 n = val.load(std::memory_order_acquire);
    if (n == NOTIFIED)
        return; // fast path
    if (n == EMPTY)
    {
        if (!val.compare_exchange_strong(n, WAITING))
        {
            if (n == NOTIFIED)
                return;
            chassert(n == WAITING);
        }
    }
    while (true)
    {
        futexWait(&val, WAITING);
        n = val.load();
        if (n == NOTIFIED)
            return;
        chassert(n == WAITING);
    }
}

void CompletionNotification::notify()
{
    UInt32 n = val.exchange(NOTIFIED);
    /// If there were no wait() calls before the notify() call, avoid the syscall.
    if (n == WAITING)
        futexWake(&val, INT32_MAX);
}

#else

bool CompletionNotification::check() const
{
    return notified.load();
}

void CompletionNotification::wait()
{
    if (!check())
        future.wait();
}

void CompletionNotification::notify()
{
    if (!notified.exchange(true))
        promise.set_value();
}

#endif

}
