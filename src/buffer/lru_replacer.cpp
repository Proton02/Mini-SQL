#include "buffer/lru_replacer.h"

// num_pages the maximum number of pages the LRUReplacer will be required to store
// num_pages 表示LRUReplacer需要存储的最大页数
LRUReplacer::LRUReplacer(size_t num_pages){
  this->num_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
// 替换（即删除）与所有被跟踪的页相比最近最少被访问的页，将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中输出并返回true，
// 如果当前没有可以替换的元素则返回false；
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 先判断当前是否有可以替换的元素，如果没有则返回false
  if(victims.empty()) {
    return false;
  }
  else {
    // 将该页帧号存储在输出参数frame_id中
    *frame_id = victims.back();
    // 删除该页帧号
    victims.pop_back();
    return true;
  }
}
/**
 * TODO: Student Implement
 */
// 将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧。
// Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 遍历lru_list，移除该数据页对应的页帧
  for(auto it = victims.begin(); it != victims.end(); it++) {
    if(*it == frame_id) {
      victims.erase(it);
      break;
    }
  }
}

/**
 * TODO: Student Implement
 */
// 将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
// Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换；
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 如果当前lru_list中的元素个数大于等于num_pages，则直接返回
  if(victims.size() >= num_pages) {
    return;
  }
  // 如果当前数据页对应的页帧已经在lru_list中，则直接返回
  // if(std::find(victims.begin(), victims.end(), frame_id) != victims.end()) {
  //   return;
  // }
  for(auto it = victims.begin(); it != victims.end(); it++) {
    if(*it == frame_id) {
      return;
    }
  }
  // 将数据页对应的页帧放入lru_list中
  victims.push_front(frame_id);
}

/**
 * TODO: Student Implement
 */
// LRUReplacer::Size()：此方法返回当前LRUReplacer中能够被替换的数据页的数量
size_t LRUReplacer::Size() {
  return victims.size();
}