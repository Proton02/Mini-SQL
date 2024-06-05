#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
// BufferPoolManager::FetchPage(page_id)：根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取；
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // unordered_map<page_id_t, frame_id_t> page_table_;  // to keep track of pages
  // Page* pages_;                                      // array of pages
  // DiskManager *disk_manager_;                        // pointer to the disk manager.
  // Replacer *replacer_;                               // to find an unpinned page for replacement
  // list<frame_id_t> free_list_;                       // to find a free page for replacement
  // recursive_mutex latch_;                            // to protect shared data structure

  // frame_id_t frame_id_temp;
  // if(page_id > MAX_VALID_PAGE_ID || page_id <= INVALID_PAGE_ID)
  //     return nullptr;
  // // 1.     Search the page table for the requested page (P).
  // bool flag = false;
  // for(auto it = page_table_.begin(); it != page_table_.end(); it++) {
  //   if(it->first == page_id) {
  //     flag = true;
  //     break;
  //   }
  // }
  // // 1.1    If P exists, pin it and return it immediately.
  // if(flag == true){
  //   frame_id_temp = page_table_[page_id];
  //   // 给指定的frame_id上锁
  //   replacer_->Pin(frame_id_temp);
  //   pages_[frame_id_temp].pin_count_++;
  //   return &pages_[frame_id_temp];
  // }
  // // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  // //        Note that pages are always found from the free list first.
  // if(free_list_.size() == 0 && replacer_->Size() == 0)
  //   return nullptr;
  // else {
  //   if(free_list_.size()>0){
  //     //pick from the head of the free list
  //     frame_id_temp = free_list_.front();
  //     //delete the frame id from free list
  //     free_list_.pop_front();
  //     //update page_table_
  //     page_table_[page_id] = frame_id_temp;
  //     // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  //     disk_manager_->ReadPage(page_id, pages_[frame_id_temp].data_);
  //     pages_[frame_id_temp].pin_count_ = 1;
  //     pages_[frame_id_temp].page_id_ = page_id;
  //     return &pages_[frame_id_temp];
  //   }
  //   else{
  //     if(replacer_->Victim(&frame_id_temp) == false)
  //       return nullptr;
  //     // 2.     If R is dirty, write it back to the disk.
  //     if(pages_[frame_id_temp].IsDirty()){
  //       disk_manager_->WritePage(pages_[frame_id_temp].GetPageId(),pages_[frame_id_temp].GetData());
  //     }
  //     // 3.     Delete R from the page table and insert P.
  //     // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  //     pages_[frame_id_temp].page_id_ = page_id;
  //     pages_[frame_id_temp].pin_count_ = 1;
  //     page_table_[page_id] = frame_id_temp;
  //     //readpage from disk
  //     disk_manager_->ReadPage(page_id,pages_[frame_id_temp].data_);
  //     return &pages_[frame_id_temp];
  //   }
  // }
  //
  //
  // return nullptr;
    frame_id_t tmp;
    //梁嘉琦加的
    if(page_id > MAX_VALID_PAGE_ID || page_id <= INVALID_PAGE_ID) return nullptr;

    //If P exists, pin it and return it immediately.
    if(page_table_.count(page_id)>0){
      tmp = page_table_[page_id];
      replacer_->Pin(tmp);
      pages_[tmp].pin_count_++;
      return &pages_[tmp];
    }
    //p dos not exist
    if(free_list_.size()>0){//from freelist
      tmp = free_list_.front();//pick from the head of the free list
      page_table_[page_id] = tmp;//update page_table_
      free_list_.pop_front();////delete the frame id from free list

      disk_manager_->ReadPage(page_id, pages_[tmp].data_);
      pages_[tmp].pin_count_ = 1;
      pages_[tmp].page_id_ = page_id;

      return &pages_[tmp];
    }
    else{//from replacer
      bool flag = replacer_->Victim(&tmp);
      if(flag==false) return nullptr;
      if(pages_[tmp].IsDirty()){//write back to the disk
        disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
      }
      //update the pages_
      pages_[tmp].page_id_ = page_id;
      pages_[tmp].pin_count_ = 1;
      page_table_[page_id] = tmp;
      //readpage from disk
      disk_manager_->ReadPage(page_id,pages_[tmp].data_);
      return &pages_[tmp];
    }

    return nullptr;
}

/**
 * TODO: Student Implement
 */
// 分配一个新的数据页，并将逻辑页号于page_id中返回
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  // If all the pages in the buffer pool are pinned, return nullptr.
  if(free_list_.size() == 0 && replacer_->Size() == 0 ) {
    return nullptr;
  }
  // Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(free_list_.size() > 0) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if(replacer_->Victim(&frame_id) == false) {
      return nullptr;
    }
    // 如果找到的页是dirty的，则将其写回磁盘
    if(pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  page_id = AllocatePage();
  // Update P's metadata, zero out memory and add P to the page table.
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  page_table_.emplace(page_id, frame_id);
  return &pages_[frame_id];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_id == INVALID_PAGE_ID)
    return true;
  // 寻找指定的page_id
  bool flag = false;
  for(auto it = page_table_.begin(); it != page_table_.end(); it++) {
    if(it->first == page_id) {
      flag = true;
      break;
    }
  }
  if(flag == false)
    return true;
  else {
    // 如果存在，就找到对应的frame_id
    frame_id_t frame_id_temp = page_table_[page_id];
    //If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if(pages_[frame_id_temp].pin_count_>0)
      return false;
    // 此时可以删除P
    page_table_.erase(page_id);
    // 需要更新metadata
    pages_[frame_id_temp].ResetMemory();
    pages_[frame_id_temp].page_id_=INVALID_PAGE_ID;
    pages_[frame_id_temp].is_dirty_=false;
    // 将P返回到free list
    free_list_.push_back(frame_id_temp);
    // call DeallocatePage
    disk_manager_->DeAllocatePage(page_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
// 取消固定一个数据页
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 判断是否为无效的page_id
  if(page_id == INVALID_PAGE_ID)
    return false;
  // 判断是否存在指定的page_id
  if(page_table_.count(page_id)==0)
    return false;
  frame_id_t frame_id_temp = page_table_[page_id];
  // 如果数据已经被取消固定
  if(pages_[frame_id_temp].pin_count_==0)
    return false;
  // 如果数据页被固定，则取消固定
  pages_[frame_id_temp].pin_count_--;
  replacer_->Unpin(frame_id_temp);
  if(is_dirty)
    pages_[frame_id_temp].is_dirty_ = true;
  return true;
}

/**
 * TODO: Student Implement
 */
// 将数据页转储到磁盘中，无论其是否被固定
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_id == INVALID_PAGE_ID)
    return false;
  if(page_table_.count(page_id) == 0)
    return false;
  frame_id_t frame_id_temp = page_table_[page_id];
  // 将数据页写入磁盘
  disk_manager_->WritePage(page_id, pages_[frame_id_temp].data_);
  pages_[frame_id_temp].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}