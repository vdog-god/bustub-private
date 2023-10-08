//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) {
    this->key_char_ = key_char;
    this->is_end_ = false;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    children_ = std::move(other_trie_node.children_);
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return (children_.count(key_char) == 1 ? true : false); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return (children_.size() > 0 ? true : false); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (children_.count(key_char) == 1) return nullptr;
    if ((*child).key_char_ != key_char) return nullptr;
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (children_.count(key_char) == 0) return nullptr;
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.count(key_char) != 0) children_.erase(key_char);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key  char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value)
      : TrieNode{std::move(trieNode)} {  // std::move后trieNode被视为一个右值
    value_ = value;
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value)
      : TrieNode(key_char) {  // 用()看上去更像一个函数，正好符合此处调用的是构造函数
    value_ = value;
    is_end_ = true;
    value_ = value;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() : root_{nullptr} { root_ = std::make_unique<TrieNode>('\0'); };

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    latch_.WLock();
    if (key.empty()) {
      latch_.WUnlock();
      return false;
    }
    auto nodeptr = &root_;
    std::unique_ptr<TrieNode> *parent = nodeptr;
    size_t i = 1;
    /* auto
    nodeptr=*root_;//这种写法错误，原因在于相当于调用拷贝构造函数来拷贝*root里的内容，而*root中有unique指针无法直接拷贝
    auto Parentptr;
    */
    for (auto &x : key) {
      if (!nodeptr->get()->HasChild(x)) {  // 无孩子节点
        if (i == key.size()) {
          std::unique_ptr<TrieNodeWithValue<T>> temp = std::make_unique<TrieNodeWithValue<T>>(x, value);
          nodeptr->get()->InsertChildNode(x, std::move(temp));
        } else {
          std::unique_ptr<TrieNode> temp = std::make_unique<TrieNode>(x);
          nodeptr->get()->InsertChildNode(x, std::move(temp));
        }
      } else {  // 有孩子节点
        if (i == key.size()) {
          parent = nodeptr;
          nodeptr = nodeptr->get()->GetChildNode(x);
          if (nodeptr->get()->IsEndNode()) {
            latch_.WUnlock();
            return false;
          }  // 进去看看该节点的孩子是否为终端节点，表示节点已存在
          if (!nodeptr->get()->IsEndNode()) {
            auto back备份 = std::move(*nodeptr);
            parent->get()->RemoveChildNode(x);
            auto temp = std::make_unique<TrieNodeWithValue<T>>(std::move(*back备份), value);
            parent->get()->InsertChildNode(x, std::move(temp));
          }
        }
      }
      parent = nodeptr;
      nodeptr = nodeptr->get()->GetChildNode(x);
      i++;
      //  nodeptr = nodeptr->get()->GetChildNode(x);
    }
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * 移除节点有三种情况：1.移除的是无孩终端端点。2.移除的是无孩的非终端节点。3移除的是有孩终端节点。(关于第三点在删除时直接设置false而不用删除派生类节点，因为指向基类的指针中保存派生类会将指向派生类直接截断后保存在基类中，因此基类指针中可以存放派生类的地址。那么可以像正常访问一个TrieNode一样访问TrieNodeWithValue，而不用将派生类转化为基类后再访问。而只要is_end标识为false，那么这个指针就不能下转为派生类，下转会失败。而将一个派生类的is_end设置为false则能够下转成功，这可能是因为编译器无法识别这个指针指向的对象，只能通过指针的类型来识别，而基类的指针可以指向派生类，因此访问基类的指针时，不管这个基类中的指针放的是不是派生类的对象，编译器只会把他识别为一个基类，如果想要识别成功，就要引入一个虚函数。因此实际上只用判断两种节点：终端和非终端。遇见终端节点时设置为false即可。)
   * 对于移除时的判断顺序应该是先判断是否是终端节点，若是终端节点则判断该节点是否有孩，无孩则直接删除并进入删除非终端结点的递归循环
   * 若有孩则只将该节点转化为trieNode节点，转化后退出递归循环。
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string
                  &key) { /*利用递归，当前层执行完毕后返回到上一层时删除节点。需要由当前层返回一个信号来允许上层删除*/
    // 先判断key是否存在
    latch_.WLock();
    bool success = false;
    auto nodeptr = &root_;
    if (Getstring(key, nodeptr)) {
      Dfs(key, 0, &root_, &success);
    }
    latch_.WUnlock();
    return success;
  }

  bool Getstring(const std::string &key, std::unique_ptr<TrieNode> *nodeptr) {  // 查找key是否存在
    for (auto &x : key) {
      nodeptr = nodeptr->get()->GetChildNode(x);
      if (nodeptr == nullptr) {
        return false;
      }
    }
    if (!nodeptr->get()->IsEndNode()) {
      return false;
    }
    return true;
  }

  bool Dfs(const std::string &key, size_t i, std::unique_ptr<TrieNode> *nodeptr, bool *success) {
    if (i == key.size()) {
      *success = true;
      nodeptr->get()->SetEndNode(false);
      return !nodeptr->get()->IsEndNode() && !nodeptr->get()->HasChildren();
    }
    bool can_remove = Dfs(key, i + 1, nodeptr->get()->GetChildNode(key[i]), success);
    if (can_remove) {
      nodeptr->get()->RemoveChildNode(key[i]);
    }
    return !nodeptr->get()->IsEndNode() && !nodeptr->get()->HasChildren();  //
  }
  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   *
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    latch_.WLock();
    if (key.empty()) {
      *success = false;
    }
    auto nodeptr = &root_;
    if (Getstring(key, nodeptr)) {
      for (auto &x : key) {
        nodeptr = nodeptr->get()->GetChildNode(x);
      }
      *success = true;
      TrieNodeWithValue<T> *temp = dynamic_cast<TrieNodeWithValue<T> *>(nodeptr->get());
      if (temp != nullptr) {
        *success = true;
        latch_.WUnlock();
        return temp->GetValue();
      }
    } else {
      *success = false;
    }
    latch_.WUnlock();
    return {};
  }
};
}  // namespace bustub
