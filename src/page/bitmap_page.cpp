#include "page/bitmap_page.h"

#include "glog/logging.h"

// static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t);
// 对应信息，除去元信息外，页中剩余的部分就是Bitmap存储的具体数据，其大小BITMAP_CONTENT_SIZE可以通过PAGE_SIZE - BITMAP_PAGE_META_SIZE来计算
// 一个bitmap page能够支持最多 BITMAP_CONTENT_SIZE * 8 个页的分配
template <size_t PageSize>
// 分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标（从0开始）；
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 判断分配的页数是否超过最大页数
  if(page_allocated_ >= MAX_CHARS * 8){
    return false;
    //full or invalid offset
  }
  // 下一个空闲页的字节索引
  uint32_t byte_index = next_free_page_ / 8;
  // 下一个空闲页的位索引
  uint8_t bit_index = next_free_page_ % 8;
  // 1 << n_bit_index = 2^(n_bit_index) = 100..000
  // 将n_byte_index字节的第n_bit_index位置1,表示该页已经被分配
  bytes[byte_index] = bytes[byte_index] | (1 << bit_index);
  // page_offset表示分配的页的偏移量
  page_offset = next_free_page_;
  // 更新下一个空闲页的索引
  for(uint32_t i = 0;i < MAX_CHARS * 8; i++){
    if((bytes[i / 8] & (1 << (i % 8))) == 0) {
      next_free_page_ = i;
      break;
    }
  }
  page_allocated_ += 1;
  return true;
}


/**
 * TODO: Student Implement
 */
template <size_t PageSize>
// 回收已经被分配的页
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 判断释放的页数是否超过最大页数
  if(page_offset >= MAX_CHARS * 8)
    return false;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  // 判断释放的页是否已经被分配
  if((IsPageFreeLow(byte_index,bit_index))){
    return false;//the page has not been allocated
  }
  // 如果没有释放，则将该页的字节索引的第bit_index位置0
  bytes[byte_index] = bytes[byte_index] & ~(1 << bit_index);
  page_allocated_-= 1;
  // 当前的offset被释放，更新next_free_page_
  next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
// 判断给定的页是否是空闲（未分配）的。
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset >= MAX_CHARS * 8)
    return false;
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ((bytes[byte_index] & (1 << bit_index)) == 0);
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;