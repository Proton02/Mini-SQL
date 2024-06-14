#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"


DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
// * Disk page storage format: (Free Page BitMap Size = PAGE_SIZE * 8, we note it as N)
// * | Meta Page | Free Page BitMap 1 | Page 1 | Page 2 | ....
// *      | Page N | Free Page BitMap 2 | Page N+1 | ... | Page 2N | ... |
// 从磁盘中分配一个空闲页，并返回空闲页的逻辑页号；
page_id_t DiskManager::AllocatePage() {
  // 获取meta_page， 类型为DiskFileMetaPage*
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // uint32_t GetExtentNums() { return num_extents_; }
  // uint32_t GetAllocatedPages() { return num_allocated_pages_; }
  // uint32_t GetExtentUsedPage(uint32_t extent_id) {
  //   if (extent_id >= num_extents_) {
  //     return 0;
  //   }
  //   return extent_used_page_[extent_id];
  // }
  // 如果已经分配的页数超过最大页数，返回INVALID_PAGE_ID
  if(meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;
  // 找到第一个可分配的page的extent
  // extent_num表示当前已经分配的extent数， GetExtentNums()返回uint32_t类型
  // extent_free表示当前可分配的extent
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t extent_free = 0;
  for(extent_free = 0; extent_free < extent_num; extent_free++) {
    if(meta_page->extent_used_page_[extent_free] < BITMAP_SIZE) {
      break;
    }
  }
  // 此时extent_free为可分配的extent，extent_free最大等于extent_num,此时意味着需要新建一个分区
  // 知道分区后，找到对应的bitmap_page
  char page_data[PAGE_SIZE];
  // 计算物理页号，每个extent的第一个page用于存储bitmap，额外加上meta_page
  page_id_t physical_page_id = extent_free * (BITMAP_SIZE + 1) + 1;
  // page_data用于存储bitmap_page
  ReadPhysicalPage(physical_page_id, page_data);
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  // 利用bitmap_page分配一个空闲页
  uint32_t page_offset = 0;
  if(!bitmap_page->AllocatePage(page_offset)) {
    LOG(ERROR) << "Allocate page failed" << std::endl;
  }
  // 将bitmap_page写回磁盘，更新bitmap_page
  WritePhysicalPage(physical_page_id, page_data);
  // 更新meta_page
  meta_page->extent_used_page_[extent_free]++;
  meta_page->num_allocated_pages_++;
  // 如果extent_used_page_[extent_free]为1，表示新建了一个extent
  if(meta_page->extent_used_page_[extent_free] == 1)
    meta_page->num_extents_++;
  // 返回逻辑页号
  return extent_free * BITMAP_SIZE + page_offset;
}

/**
 * TODO: Student Implement
 */
// 释放磁盘中逻辑页号对应的物理页。
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto meta_page = reinterpret_cast<DiskFileMetaPage *> (meta_data_);
  // logical_page__id = i * BITMAP_SIZE + offset
  // physical_page_id = i * (BITMAP_SIZE + 1) + 1 + offset
  // 已知逻辑页号，计算物理页号
  page_id_t physical_page_id = (logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1;
  // 没有offset，计算的是bitmap_page的物理页号
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_page_id, page_data);
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  // 计算出page_offset
  // bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset)
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;
  if(!bitmap_page->DeAllocatePage(page_offset)) {
    LOG(ERROR) << "Deallocate page failed" << std::endl;
  }
  else {
    // 释放成功
    WritePhysicalPage(physical_page_id, page_data);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
  }


}

/**
 * TODO: Student Implement
 */
// 判断该逻辑页号对应的数据页是否空闲。
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if(logical_page_id > MAX_VALID_PAGE_ID)
    return false;
  // 先计算物理页号，然后读取bitmap_page，判断该页是否空闲
  page_id_t physical_page_id = (logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1;
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(physical_page_id, page_data);
  auto bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);
  // bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const
  uint32_t offset = logical_page_id % BITMAP_SIZE;
  return bitmap_page->IsPageFree(offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  if(logical_page_id > MAX_VALID_PAGE_ID)
    return INVALID_PAGE_ID;
  // logic_page_id = i * BITMAP_SIZE + offset
  uint32_t offset = logical_page_id % BITMAP_SIZE;
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  return extent_id * (BITMAP_SIZE + 1) + 1 + offset + 1;
}
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
