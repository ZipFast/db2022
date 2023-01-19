//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/index/index.h"
#include "storage/page/page.h"
#include "common/logger.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1), dir_({std::make_shared<Bucket>(bucket_size)}) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t index, const K& key) -> void {
  auto globalDepth = GetGlobalDepthInternal();
  if (globalDepth == bucket->GetDepth()) {
     for (auto i = 0; i < num_buckets_; i++) {
       dir_.push_back(dir_[i]);
     }
     global_depth_++;
     num_buckets_ *= 2;
  }
  auto highBit = GetHighestBit(bucket);
  size_t localIndex = (std::hash<K>()(key) & highBit - 1);
  dir_[localIndex+highBit] = std::make_shared<Bucket>(bucket_size_);
  auto bucket0 = dir_[localIndex];
  auto bucket1 = dir_[localIndex+highBit];
  bucket0->IncrementDepth();
  bucket1->SetDepth(bucket0->GetDepth());

  size_t size = bucket0->GetSize();
  for (size_t i = 0; i < size; i++) {
    auto item = bucket0->Front();

    bucket0->PopFront();

    auto h = std::hash<K>()(item.first) & highBit;
    if (h)
      bucket1->PushBack(std::make_pair(item.first, item.second));
    else
      bucket0->PushBack(std::make_pair(item.first, item.second));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  for (;;) {
    size_t index = IndexOf(key);
    auto bucket = dir_[index];
    if (!bucket->IsFull()) {
      bucket->Insert(key, value);
      break;
    }
    if (bucket->Insert(key, value)) {
      break;
    }
    RedistributeBucket(bucket, index, key);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {

  auto iter = std::find_if(list_.cbegin(), list_.cend(), [&](const std::pair<K, V> &x) { return x.first == key; });
  if (iter == list_.cend()) {
    return false;
  }
  value = iter->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto iter = std::find_if(list_.cbegin(), list_.cend(), [&](const std::pair<K, V> &x) { return x.first == key; });
  if (iter == list_.cend()) {
    return false;
  }
  list_.remove(*iter);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto iter = std::find_if(list_.begin(), list_.end(), [&](const std::pair<K, V> &x) { return x.first == key; });
  if (iter == list_.end()) {
    if (IsFull()) {
      return false;
    }
    list_.push_back({key, value});
    return true;
  }
  *iter = std::make_pair(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
