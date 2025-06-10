#pragma once

#include <memory>

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Formats/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
    extern const Event MarkCacheEvictedBytes;
    extern const Event MarkCacheEvictedMarks;
    extern const Event MarkCacheEvictedFiles;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t MARK_CACHE_OVERHEAD = 128;

    size_t operator()(const MarksInCompressedFile & marks) const
    {
        return marks.approximateMemoryUsage() + MARK_CACHE_OVERHEAD;
    }
};

extern template class CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;
/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
    using Base = CacheBase<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
    MarkCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Calculate key from path to file.
    static UInt128 hash(const String & path_to_file);

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result.first;
    }

private:
    /// Called for each individual cell being evicted from cache
    void onValueRemoval(const MappedPtr & mappedPtr) override
    {
        /// File is the key of MarkCache, each removal means eviction of 1 file from the cache.
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedFiles);

        auto marks_in_compressed_file = std::static_pointer_cast<MarksInCompressedFile>(mappedPtr);
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedBytes, MarksWeightFunction()(*marks_in_compressed_file));
        ProfileEvents::increment(ProfileEvents::MarkCacheEvictedMarks, marks_in_compressed_file->getNumberOfMarks());
    }

};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
