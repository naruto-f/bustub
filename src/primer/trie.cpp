#include "primer/trie.h"
#include <string_view>
#include <stack>
#include "common/exception.h"



namespace bustub {

std::shared_ptr<TrieNode> DeepCopy(std::shared_ptr<const TrieNode> old_root) {
  //std::unique_ptr<TrieNode> dup_ptr = old_root->Clone();
  std::shared_ptr<TrieNode> new_root;
  std::map<char, std::shared_ptr<const TrieNode>> new_children;
  for (auto& iter : old_root->children_) {
    new_children[iter.first] = DeepCopy(iter.second);
  }

  new_root->children_ = new_children;
  return new_root;
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (!root_) {
    return nullptr;
  }

  int key_size = key.size(), cur_pos = 0;
  std::shared_ptr<const TrieNode> cur_node = root_;

  for (; cur_pos < key_size; ++cur_pos) {
    char c = key[cur_pos];
    if (!cur_node->children_.count(c)) {
      return nullptr;
    }

    cur_node = cur_node->children_.at(c);
  }

  if (!cur_node || !cur_node->is_value_node_) {
    return nullptr;
  }

  const TrieNodeWithValue<T>* value_node = dynamic_cast<const TrieNodeWithValue<T>*>(cur_node.get());
  if (!value_node) {
    return nullptr;
  }

  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (!root_) {
    root_.reset(new TrieNode());
  }

  std::shared_ptr<const TrieNode> new_root = DeepCopy(root_);
  int key_size = key.size(), cur_pos = 0;
  std::shared_ptr<const TrieNode> cur_node = new_root;
  const TrieNode *prev_node(nullptr);

  for (; cur_pos < key_size; ++cur_pos) {
    prev_node = cur_node.get();
    char c = key[cur_pos];
    if (cur_node->children_.count(c)) {
      cur_node = cur_node->children_.at(c);
    } else {
      std::shared_ptr<const TrieNode> next_node(nullptr);
      if (cur_pos == key_size - 1) {
        next_node = std::make_shared<const TrieNodeWithValue<T>>(nullptr);
      } else {
        next_node = std::make_shared<const TrieNode>();
      }

      TrieNode* cur_node_ptr = const_cast<TrieNode*>(cur_node.get());
      cur_node_ptr->children_.insert(std::pair<char, std::shared_ptr<const TrieNode>>(c, next_node));
      cur_node = next_node;
    }
  }

  TrieNodeWithValue<T>* value_node = const_cast<TrieNodeWithValue<T>*>(dynamic_cast<const TrieNodeWithValue<T>*>(cur_node.get()));
  if (!value_node || !cur_node->is_value_node_) {
    std::shared_ptr<const TrieNode> new_node = std::make_shared<const TrieNodeWithValue<T>>(cur_node->children_, std::make_shared<T>(std::move(value)));
    if (!prev_node) {
      new_root = std::move(new_node);
    } else {
      const_cast<TrieNode*>(prev_node)->children_[key[key_size - 1]] = std::move(new_node);
    }
  } else {
    value_node->value_ = std::make_shared<T>(std::move(value));
  }

  return Trie(new_root);
}


auto Trie::Remove(std::string_view key) const -> Trie {
  //throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (!root_) {
    root_.reset(new TrieNode());
  }

  if (key.size() == 0) {
    return *this;
  }

  int key_size = key.size(), cur_pos = 0;
  std::shared_ptr<TrieNode> new_root = DeepCopy(root_);
  std::shared_ptr<const TrieNode> cur_node = new_root;

  std::stack<std::pair<TrieNode*, char>> st;

  for (; cur_pos < key_size; ++cur_pos) {
    char c = key[cur_pos];
    if (!cur_node->children_.count(c)) {
      return *this;
    }
    st.push({const_cast<TrieNode*>(cur_node.get()), c});
    cur_node = cur_node->children_.at(c);
  }

  if (!cur_node->is_value_node_) {
    return *this;
  }

  if (!cur_node->children_.empty()) {
    std::shared_ptr<const TrieNode> new_node = std::make_shared<const TrieNode>(cur_node->children_);
    st.top().first->children_[st.top().second] = new_node;
  } else {
    while (!st.empty()) {
      std::pair<TrieNode*, char> cur_pair = st.top();
      st.pop();
      cur_pair.first->children_.erase(cur_pair.second);
      if (!cur_pair.first->children_.empty() || cur_pair.first->is_value_node_) {
        break;
      }
    }
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;


}  // namespace bustub
