#include <Common/threadPoolCallbackRunner.h>

namespace DB
{

ThreadPoolCallbackRunnerFast::ThreadPoolCallbackRunnerFast() = default;

void ThreadPoolCallbackRunnerFast::initThreadPool(ThreadPool & pool_, size_t max_threads_, std::string thread_name_, ThreadGroupPtr thread_group_)
{
    chassert(!pool);
    mode = Mode::ThreadPool;
    pool = &pool_;
    max_threads = max_threads_;
    thread_name = thread_name_;
    thread_group = thread_group_;

    /// TODO [parquet]: Maybe adjust number of threads dynamically based on queue size.
    threads = max_threads;
    for (size_t i = 0; i < max_threads; ++i)
        pool->scheduleOrThrowOnError([this] { threadFunction(); });
}

ThreadPoolCallbackRunnerFast::ThreadPoolCallbackRunnerFast(Mode mode_) : mode(mode_)
{
    chassert(mode != Mode::ThreadPool);
}

ThreadPoolCallbackRunnerFast::~ThreadPoolCallbackRunnerFast()
{
    shutdown();
}

void ThreadPoolCallbackRunnerFast::shutdown()
{
    /// May be called twice.
    std::unique_lock lock(mutex);
    shutdown_requested = true;
    queue_cv.notify_all();
    shutdown_cv.wait(lock, [&] { return threads == 0; });
}

void ThreadPoolCallbackRunnerFast::operator()(std::function<void()> f)
{
    if (mode == Mode::Disabled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread pool runner is not initialized");

    {
        std::unique_lock lock(mutex);
        queue.push_back(std::move(f));
    }
    queue_cv.notify_one();
}

void ThreadPoolCallbackRunnerFast::bulkSchedule(std::vector<std::function<void()>> fs)
{
    if (fs.empty())
        return;

    if (mode == Mode::Disabled)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread pool runner is not initialized");

    {
        std::unique_lock lock(mutex);
        queue.insert(queue.end(), std::move_iterator(fs.begin()), std::move_iterator(fs.end()));
    }

    if (fs.size() <= 2 || fs.size() * 4 < max_threads) // (numbers chosen at random, not optimized)
    {
        for (size_t i = 0; i < fs.size(); ++i)
            queue_cv.notify_one();
    }
    else
    {
        queue_cv.notify_all();
    }
}

bool ThreadPoolCallbackRunnerFast::runTaskInline()
{
    std::function<void()> f;
    {
        std::unique_lock lock(mutex);
        if (queue.empty())
            return false;
        f = std::move(queue.front());
        queue.pop_front();
    }
    f();
    return true;
}

void ThreadPoolCallbackRunnerFast::threadFunction()
{
    ThreadGroupSwitcher switcher(thread_group, thread_name.c_str());

    std::unique_lock lock(mutex);
    while (true)
    {
        chassert(lock.owns_lock());
        if (shutdown_requested)
        {
            threads -= 1;
            if (threads == 0)
                shutdown_cv.notify_all();
            return;
        }

        if (queue.empty())
        {
            queue_cv.wait(lock);
            continue;
        }

        std::function<void()> f = std::move(queue.front());
        queue.pop_front();

        lock.unlock();

        try
        {
            f();

            CurrentThread::updatePerformanceCountersIfNeeded();
        }
        catch (...)
        {
            tryLogCurrentException("FastThreadPool");
            chassert(false);
        }

        lock.lock();
    }
}

template ThreadPoolCallbackRunnerUnsafe<void> threadPoolCallbackRunnerUnsafe<void>(ThreadPool &, const std::string &);
template class ThreadPoolCallbackRunnerLocal<void>;

}
