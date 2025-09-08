#include "storage/btree/tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"

#include <cstring>
#include <memory>
#include <tuple>

#define ACCESS_RECORD_MACRO(node, pos)                                         \
  ({                                                                           \
    auto node_ptr = *((node).Ptr());                                           \
    std::memcpy((key), node_ptr.GetPrefix(), node_ptr.prefix_len);             \
    std::memcpy((key) + node_ptr.prefix_len, node_ptr.GetKey(pos),             \
                node_ptr.slots[pos].key_length);                               \
    fn({(key), (key_len)}, node_ptr.GetPayload(pos));                          \
  })

namespace leanstore::storage {

leng_t BTree::btree_slot_counter = 0;

BTree::BTree(buffer::BufferManager *buffer_pool, bool append_bias)
    : buffer_(buffer_pool), append_bias_(append_bias) {
  GuardX<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
  GuardX<BTreeNode> root_page(buffer_, buffer_->AllocPage());
  new (root_page.Ptr()) storage::BTreeNode(true, metadata_slotid_);
  metadata_slotid_ = btree_slot_counter++;
  meta_page->roots[metadata_slotid_] = root_page.PageID();
  // -------------------------------------------------------------------------------------
  meta_page.AdvanceGSN();
  root_page.AdvanceGSN();
}

void BTree::ToggleAppendBiasMode(bool append_bias) {
  append_bias_ = append_bias;
}

void BTree::SetComparisonOperator(ComparisonLambda cmp_op) {
  cmp_lambda_ = cmp_op;
}

auto BTree::IterateAllNodes(GuardO<BTreeNode> &node,
                            const std::function<u64(BTreeNode &)> &inner_fn,
                            const std::function<u64(BTreeNode &)> &leaf_fn)
    -> u64 {
  if (!node->IsInner()) {
    return leaf_fn(*(node.Ptr()));
  }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->count; idx++) {
    GuardO<BTreeNode> child(buffer_, node->GetChild(idx));
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  GuardO<BTreeNode> child(buffer_, node->right_most_child);
  res += IterateAllNodes(child, inner_fn, leaf_fn);
  return res;
}

auto BTree::IterateUntils(GuardO<BTreeNode> &node,
                          const std::function<bool(BTreeNode &)> &inner_fn,
                          const std::function<bool(BTreeNode &)> &leaf_fn)
    -> bool {
  if (!node->IsInner()) {
    return leaf_fn(*(node.Ptr()));
  }

  auto res = inner_fn(*(node.Ptr()));
  if (!res) {
    for (auto idx = 0; idx < node->count; idx++) {
      GuardO<BTreeNode> child(buffer_, node->GetChild(idx));
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
      if (res) {
        break;
      }
    }
    if (!res) {
      GuardO<BTreeNode> child(buffer_, node->right_most_child);
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
    }
  }
  return res;
}

// -------------------------------------------------------------------------------------
auto BTree::FindLeafOptimistic(std::span<u8> key) -> GuardO<BTreeNode> {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  while (node->IsInner()) {
    node = GuardO<BTreeNode>(buffer_, node->FindChild(key, cmp_lambda_), node);
  }
  return node;
}

auto BTree::FindLeafShared(std::span<u8> key) -> GuardS<BTreeNode> {
  while (true) {
    try {
      GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

      while (node->IsInner()) {
        node =
            GuardO<BTreeNode>(buffer_, node->FindChild(key, cmp_lambda_), node);
      }
      return GuardS<BTreeNode>(std::move(node));
    } catch (const sync::RestartException &) {
    }
  }
}

void BTree::TrySplit(GuardX<BTreeNode> &&parent, GuardX<BTreeNode> &&node) {
  // create new root if necessary
  if (parent.PageID() == METADATA_PAGE_ID) {
    auto meta_p = reinterpret_cast<MetadataPage *>(parent.Ptr());
    // Root node is full, alloc a new root
    GuardX<BTreeNode> new_root(buffer_, buffer_->AllocPage());
    new (new_root.Ptr()) storage::BTreeNode(false, metadata_slotid_);
    new_root->right_most_child = node.PageID();
    if (FLAGS_wal_enable) {
      new_root.ReserveWalEntry<WALNewRoot>(0);
      new_root.SubmitActiveWalEntry();
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent = std::move(new_root);
    parent.AdvanceGSN();
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(append_bias_.load(), cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);
  pageid_t next_pid = node->next_leaf_node;
  pageid_t prev_pid = node->prev_leaf_node;

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    GuardX<BTreeNode> new_child(buffer_, buffer_->AllocPage());
    new (new_child.Ptr())
        storage::BTreeNode(!node->IsInner(), metadata_slotid_);
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(),
                    new_child.PageID(), sep_info.slot, {sep_key, sep_info.len},
                    cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    pageid_t new_pid = new_child.PageID();

    if (node->is_leaf && new_child->is_leaf) {
      Ensure(new_child.PageID() == node->next_leaf_node);
      Ensure(node.PageID() == new_child->prev_leaf_node);
      Ensure(new_child->HasLeftNeighbor());
      Ensure(node->HasRightNeighbor());
      Ensure(next_pid == new_child->next_leaf_node);
      Ensure(node->prev_leaf_node == prev_pid);

      if (new_child->HasRightNeighbor()) {
        GuardX<BTreeNode> right_locked(buffer_, new_child->next_leaf_node);
        if (right_locked->is_leaf) {
          right_locked->prev_leaf_node = new_child.PageID();
          right_locked.Unlock();
        }
      } else {
        // new_child is the rightmost child
        Ensure(parent->right_most_child == new_child.PageID());
        Ensure(next_pid == BTreeNodeHeader::EMPTY_NEIGHBOR);
        parent->right_most_child = new_child.PageID();
        // Ensure(new_child->IsUpperFenceInfinity());
      }
    }

    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL new node
      new_child.ReserveWalEntry<WALInitPage>(0);
      new_child.SubmitActiveWalEntry();
      // WAL logical split
      //  all parent, node, and new_child share the same local log buffer,
      //  hence we don't need to push this wal entry using parent's and
      //  new_child's
      auto &entry = node.ReserveWalEntry<WALLogicalSplit>(0);
      std::tie(entry.parent_pid, entry.left_pid, entry.right_pid,
               entry.sep_slot) =
          std::make_tuple(parent.PageID(), node.PageID(), new_child.PageID(),
                          sep_info.slot);
      node.SubmitActiveWalEntry();
    }
    return;
  }

  // must split parent to make space for separator, restart from root to do this
  node.Unlock();

  EnsureSpaceForSplit(parent.UnlockAndGetPtr(), {sep_key, sep_info.len});
}

void BTree::EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key) {
  assert(to_split->IsInner());
  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_,
                             reinterpret_cast<MetadataPage *>(parent.Ptr())
                                 ->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner() && (node.Ptr() != to_split)) {
        parent = std::move(node);
        node = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_),
                                 parent);
      }

      if (node.Ptr() == to_split) {
        if (node->HasSpaceForKV(key.size(), sizeof(pageid_t))) {
          // someone else did split concurrently
          node.ValidateOrRestart();
          return;
        }

        GuardX<BTreeNode> parent_locked(std::move(parent));
        GuardX<BTreeNode> node_locked(std::move(node));
        TrySplit(std::move(parent_locked), std::move(node_locked));

        // complete, get out of the optimistic loop
      }
      return;
    } catch (const sync::RestartException &) {
    }
  }
}

void BTree::EnsureUnderfullInnersForMerge(BTreeNode *to_merge) {
  assert(to_merge->IsInner());
  auto rep_key = to_merge->GetUpperFence();
  while (true) {
    try {
      // TODO(Duy): Implement tree-level compression
      //  i.e. parent of parent is page 0 (i.e. metadata page)
      //  and we can compress the inner nodes
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_,
                             reinterpret_cast<MetadataPage *>(parent.Ptr())
                                 ->GetRoot(metadata_slotid_),
                             parent);

      leng_t node_pos = 0;
      while (node->IsInner() && (node.Ptr() != to_merge)) {
        parent = std::move(node);
        node = GuardO<BTreeNode>(
            buffer_, parent->FindChild(rep_key, node_pos, cmp_lambda_), parent);
      }

      if (parent.PageID() != METADATA_PAGE_ID && // Root node can't be merged
          node.Ptr() == to_merge && // Found the correct node to be merged
          node_pos <
              parent->count &&  // Current node is not the right most child
          parent->count >= 1 && // Parent has more than one children
          (node->FreeSpaceAfterCompaction() >=
           BTreeNodeHeader::SIZE_UNDER_FULL) // Current node is underfull
      ) {
        // underfull
        auto right_pid = (node_pos < parent->count - 1)
                             ? parent->GetChild(node_pos + 1)
                             : parent->right_most_child;
        GuardO<BTreeNode> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >=
            BTreeNodeHeader::SIZE_UNDER_FULL) {
          GuardX<BTreeNode> parent_locked(std::move(parent));
          GuardX<BTreeNode> node_locked(std::move(node));
          GuardX<BTreeNode> right_locked(std::move(right));
          if (!TryMerge(std::move(parent_locked), std::move(node_locked),
                        std::move(right_locked), node_pos)) {
          }
        }
      }
      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {
    }
  }
}

auto BTree::LookUp(std::span<u8> key, const PayloadFunc &read_cb) -> bool {
  while (true) {
    try {
      GuardO<BTreeNode> node = FindLeafOptimistic(key);
      bool found;
      leng_t pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) {
        return false;
      }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {
    }
  }
}

void BTree::Insert(std::span<u8> key, std::span<const u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_,
                             reinterpret_cast<MetadataPage *>(parent.Ptr())
                                 ->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_),
                                 parent);
      }

      // Found the leaf node to insert new data
      if (node->HasSpaceForKV(key.size(), payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        // --------------------------------------------------------------------------
        // WAL Insert
        if (FLAGS_wal_enable) {
          WalNewTuple(node_locked, WALInsert, key, payload);
        }
        // --------------------------------------------------------------------------
        return; // success
      }

      // The leaf node doesn't have enough space, we have to split it
      GuardX<BTreeNode> parent_locked(std::move(parent));
      GuardX<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));
      // We haven't run the insertion yet, so we run the loop again to insert
      // the record
    } catch (const sync::RestartException &) {
    }
  }
}

auto BTree::Remove(std::span<u8> key) -> bool {
  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_,
                             reinterpret_cast<MetadataPage *>(parent.Ptr())
                                 ->GetRoot(metadata_slotid_),
                             parent);

      leng_t node_pos = 0;
      while (node->IsInner()) {
        parent = std::move(node);
        node = GuardO<BTreeNode>(
            buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) {
        // Key not found
        return false;
      }

      auto payload = node->GetPayload(slot_id);
      leng_t entry_size = node->slots[slot_id].key_length + payload.size();

      bool merged = false;
      pageid_t merged_pid;

      if (node->FreeSpaceAfterCompaction() + entry_size >=
              BTreeNodeHeader::SIZE_UNDER_FULL &&
          //   BTreeNodeHeader::SIZE_UNDER_FULL && // Node becomes underfull
          parent.PageID() != METADATA_PAGE_ID && // Not the root node
          parent->count >= 2) {                  // Parent has enough children

        if ((node_pos + 1) < parent->count - 1) { // parent->count -1
          // // Handle right sibling
          GuardX<BTreeNode> parent_locked(std::move(parent));
          GuardX<BTreeNode> node_locked(std::move(node));
          GuardX<BTreeNode> right_locked(buffer_,
                                         parent_locked->GetChild(node_pos + 1));
          parent.ValidateOrRestart();
          Ensure(node_locked->RemoveSlot(slot_id));
          if (FLAGS_wal_enable) {
            WalNewTuple(node_locked, WALRemove, key, payload);
          }

          if (node_locked->next_leaf_node == right_locked.PageID() &&
              right_locked->FreeSpaceAfterCompaction() +
                      node_locked->FreeSpaceAfterCompaction() >=
                  PAGE_SIZE - sizeof(storage::PageHeader) &&
              right_locked->is_leaf) {
            if (TryMerge(std::move(parent_locked), std::move(node_locked),
                         std::move(right_locked), node_pos)) {
              merged = true;
              merged_pid = node_locked.PageID();
            }
          }

        } else {
          // No merge needed, simply remove the slot
          GuardX<BTreeNode> node_locked(std::move(node));
          parent.ValidateOrRestart();
          node_locked->RemoveSlot(slot_id);
          if (FLAGS_wal_enable) {
            WalNewTuple(node_locked, WALRemove, key, payload);
          }
        }
        return true;
      } else {
        // No merge needed, simply remove the slot
        GuardX<BTreeNode> node_locked(std::move(node));
        node_locked->RemoveSlot(slot_id);
        if (FLAGS_wal_enable) {
          WalNewTuple(node_locked, WALRemove, key, payload);
        }
      }
      parent.ValidateOrRestart();
      return true;
    } catch (const sync::RestartException &) {
      // Retry loop on RestartException
    }
  }
}

// vanilla
// auto BTree::Remove(std::span<u8> key) -> bool {
//   while (true) {
//     try {
//       GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
//       GuardO<BTreeNode> node(buffer_,
//                              reinterpret_cast<MetadataPage *>(parent.Ptr())
//                                  ->GetRoot(metadata_slotid_),
//                              parent);

//       leng_t node_pos = 0;
//       while (node->IsInner()) {
//         parent = std::move(node);
//         node = GuardO<BTreeNode>(
//             buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
//       }

//       bool found;
//       auto slot_id = node->LowerBound(key, found, cmp_lambda_);
//       if (!found) {
//         return false;
//       }

//       auto payload = node->GetPayload(slot_id);
//       leng_t entry_size = node->slots[slot_id].key_length + payload.size();
//       if ((node->FreeSpaceAfterCompaction() + entry_size >=
//            BTreeNodeHeader::SIZE_UNDER_FULL) && // new node is under full
//           (parent.PageID() !=
//            METADATA_PAGE_ID) &&            // current node is not the root
//            node
//           (parent->count >= 2) &&          // parent has more than one
//           children
//           ((node_pos + 1) < parent->count) // current node has a right
//           sibling
//       ) {
//         // underfull
//         GuardX<BTreeNode> parent_locked(std::move(parent));
//         GuardX<BTreeNode> node_locked(std::move(node));
//         GuardX<BTreeNode> right_locked(buffer_,
//                                        parent_locked->GetChild(node_pos +
//                                        1));
//         Ensure(node_locked->RemoveSlot(slot_id));
//         //
//         --------------------------------------------------------------------------
//         // WAL Remove
//         if (FLAGS_wal_enable) {
//           WalNewTuple(node_locked, WALRemove, key, payload);
//         }
//         //
//         --------------------------------------------------------------------------
//         // right child is also under full
//         if (right_locked->FreeSpaceAfterCompaction() >=
//             BTreeNodeHeader::SIZE_UNDER_FULL) {
//           if (!TryMerge(std::move(parent_locked), std::move(node_locked),
//                         std::move(right_locked), node_pos)) {
//             LOG_INFO("trymerge failed");
//           }
//         }
//         parent.ValidateOrRestart();
//       } else {
//         GuardX<BTreeNode> node_locked(std::move(node));
//         parent.ValidateOrRestart();
//         node_locked->RemoveSlot(slot_id);
//         //
//         --------------------------------------------------------------------------
//         // WAL Remove
//         if (FLAGS_wal_enable) {
//           WalNewTuple(node_locked, WALRemove, key, payload);
//         }
//         //
//         --------------------------------------------------------------------------
//       }
//       return true;
//     } catch (const sync::RestartException &) {
//     }
//   }
// }

bool BTree::TryMerge(GuardX<BTreeNode> &&parent, GuardX<BTreeNode> &&left,
                     GuardX<BTreeNode> &&right, leng_t left_pos) {
  pageid_t dealloc_pid = left.PageID();
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    buffer_->GetFreePageManager()->PrepareFreeTier(left.PageID(), 0);
    // -------------------------------------------------------------------------------------
    bool deallocated = false;
    if (left->is_leaf && right->is_leaf) {
      Ensure(right->prev_leaf_node == left->prev_leaf_node);
      if (right->HasLeftNeighbor()) {
        GuardX<BTreeNode> left_locked(buffer_, right->prev_leaf_node);
        left_locked->next_leaf_node = right.PageID();
        deallocated = true;
        left->lower_fence.len = 0;
        left->upper_fence.len = 0;
        buffer_->DeallocPage(dealloc_pid);
        left_locked.Unlock();
      } else {
        // No left neighbor; proceed without locking
        Ensure(right->prev_leaf_node == BTreeNodeHeader::EMPTY_NEIGHBOR);
        deallocated = true;
        left->lower_fence.len = 0;
        left->upper_fence.len = 0;
        buffer_->DeallocPage(dealloc_pid);
      }

      if (FLAGS_wal_enable) {
        // WAL merge left into right
        auto &entry = left.ReserveWalEntry<WALMergeNodes>(0);
        std::tie(entry.parent_pid, entry.left_pid, entry.right_pid,
                 entry.left_pos) =
            std::make_tuple(parent.PageID(), left.PageID(), right.PageID(),
                            left_pos);
        left.SubmitActiveWalEntry();

        auto &entry1 = right.ReserveWalEntry<WALMergeNodes>(0);
        std::tie(entry.parent_pid, entry.left_pid, entry.right_pid,
                 entry.left_pos) =
            std::make_tuple(parent.PageID(), left.PageID(), right.PageID(),
                            left_pos);
        right.SubmitActiveWalEntry();
      }
    }

    if (parent->FreeSpaceAfterCompaction() >=
        BTreeNodeHeader::SIZE_UNDER_FULL) {
      left.Unlock();
      right.Unlock();
      // Parent node is underfull, try merge this inner node
      EnsureUnderfullInnersForMerge(parent.UnlockAndGetPtr());
    }
    return true;
  } else {
    return false;
  }
}

auto BTree::Update(std::span<u8> key, std::span<const u8> payload) -> bool {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_,
                             reinterpret_cast<MetadataPage *>(parent.Ptr())
                                 ->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_),
                                 parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) {
        return false;
      }
      auto curr_payload = node->GetPayload(slot_id);

      // Found the leaf node to insert new data
      if (payload.size() <= curr_payload.size() ||
          node->HasSpaceForKV(key.size(),
                              payload.size() - curr_payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        // --------------------------------------------------------------------------
        // WAL Insert
        if (FLAGS_wal_enable) {
          WalNewTuple(node_locked, WALInsert, key, payload);
          //  WalNewTuple(node_locked, WALRemove, key, curr_payload);
        }
        // --------------------------------------------------------------------------
        node_locked.Unlock();
        parent.ValidateOrRestart();
        return true; // success
      }

      // The leaf node doesn't have enough space, we have to split it
      GuardX<BTreeNode> parent_locked(std::move(parent));
      GuardX<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));
      parent.ValidateOrRestart();

      // We haven't run the insertion yet, so we run the loop again to insert
      // the record
    } catch (const sync::RestartException &) {
    }
  }
}

auto BTree::UpdateInPlace(std::span<u8> key, const PayloadFunc &func) -> bool {
  while (true) {
    try {
      auto node = FindLeafOptimistic(key);
      bool found;
      auto pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) {
        return false;
      }

      {
        GuardX<BTreeNode> node_locked(std::move(node));
        func(node_locked->GetPayload(pos));
        // --------------------------------------------------------------------------
        if (FLAGS_wal_enable) {
          // WAL Update
          auto payload = node_locked->GetPayload(pos);
          WalNewTuple(node_locked, WALInsert, key, payload);
          // WalNewTuple(node_locked, WALRemove, key, payload);
        }
        node_locked.Unlock();
        return true;
      }
    } catch (const sync::RestartException &) {
    }
  }
}
void BTree::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) {
beginning:
  auto node = FindLeafShared(key);
  bool found;
  auto pos = node->LowerBound(key, found, cmp_lambda_);

  while (true) {
    if (buffer_->BufferFrame(node.PageID()).deallocated) {
      // fprintf(stderr, "this should never happen\n");
      goto beginning;
    }

    if (pos < node->count) {
      size_t key_len = node->prefix_len + node->slots[pos].key_length;
      u8 key[key_len];

      if (!ACCESS_RECORD_MACRO(node, pos)) {
        node.Unlock();
        return; // Exit if the macro fails
      }
      pos++; // Move to the next position
    } else {
      if (!node->HasRightNeighbor()) {
        node.Unlock();
        return; // No right neighbor, end the scan
      }

      if (node->count == 0) {
        node.Unlock();
        return; // No right neighbor, end the scan
      }

      if (node->IsUpperFenceInfinity()) {
        // Ensure(node->IsUpperFenceInfinity());
        node.Unlock();
        return; // No right neighbor, end the scan
      }

      pos = 0;
      pageid_t cur_pid = node.PageID();
      pageid_t next_pid = node->next_leaf_node;

      if (buffer_->BufferFrame(next_pid).deallocated) {
        // LOG_INFO("pid %lu %s" ,node.PageID(), node->ToString().c_str());
        //  /node->next_leaf_node = BTreeNodeHeader::EMPTY_NEIGHBOR;
        node.Unlock();
        return;
      }
      node.Unlock();
      node = (GuardS<BTreeNode>(buffer_, next_pid)); // Move to the next node
      // node = std::move(next_node);
      // Ensure(node->prev_leaf_node == cur_pid);
    }
  }
}

void BTree::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) {
  auto node = FindLeafShared(key);
  bool found;
  int pos = static_cast<int>(node->LowerBound(key, found, cmp_lambda_));
  // LowerBound search always return the first position whose key >= the search
  // key hence, if LowerBound doesn't give an exact match,
  //    then the found key will > search key as we scan desc,
  // any key > search key should be overlooked, i.e. start from pos - 1
  if (!found) {
    pos--;
  }
  while (true) {
    while (pos >= 0) {
      if (pos < node->count) {
        size_t key_len = node->prefix_len + node->slots[pos].key_length;
        u8 key[key_len];

        if (!ACCESS_RECORD_MACRO(node, pos)) {
          node.Unlock();
          return;
        }
      }
      pos--;
    }
    if (node->IsLowerFenceInfinity()) {
      // scanned until the last node
      node.Unlock();
      return;
    }
    node = FindLeafShared(node->GetLowerFence());
    pageid_t prev_leaf = node->prev_leaf_node;
    pageid_t cur_pid = node.PageID();
    // node = GuardS<BTreeNode>(buffer_, node->prev_leaf_node); // Move to the
    // next node
    pos = node->count - 1;
  }
}

auto BTree::CountEntries() -> u64 {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(
      node, [](BTreeNode &) { return 0; },
      [](BTreeNode &node) { return node.count; });
}

// -------------------------------------------------------------------------------------

auto BTree::IsNotEmpty() -> bool {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateUntils(
      node, [](BTreeNode &) { return false; },
      [](BTreeNode &node) { return (node.count > 0); });
}

auto BTree::CountPages() -> u64 {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(
      node, [](BTreeNode &) { return 1; }, [](BTreeNode &) { return 1; });
}

auto BTree::SizeInMB() -> float {
  return (CountPages()) * static_cast<float>(PAGE_SIZE) / MB;
}

bool BTree::UpdatePID(pageid_t pid, u8 *payload) {
  GuardX<BTreeNode> node(buffer_, pid);
  // LOG_INFO("pid: %lu p_gsn: %lu", node.PageID(), node->p_gsn);
  buffer_->MakePageDirty(pid);

  // Create spans for the key and payload for the first slot
  std::memcpy(reinterpret_cast<u8 *>(node.Ptr()) + sizeof(storage::PageHeader),
              payload, PAGE_SIZE - sizeof(storage::PageHeader));

  if (FLAGS_wal_enable) {
    std::span<u8> key(node->GetKey(0), node->slots[0].key_length);
    std::span<u8> payload_tuple = node->GetPayload(0);
    WalNewTuple(node, WALInsert, key, payload_tuple);
  }

  node.Unlock();

  return true;
}

bool BTree::AllocPID(pageid_t pid, u8 *payload) {
  GuardX<BTreeNode> node(buffer_, buffer_->AllocPage());

  std::memcpy(reinterpret_cast<u8 *>(node.Ptr()) + sizeof(storage::PageHeader),
              payload, PAGE_SIZE - sizeof(storage::PageHeader));

  if (FLAGS_wal_enable) {
    // WAL new node
    node.ReserveWalEntry<WALInitPage>(0);
    node.SubmitActiveWalEntry();
  }

  // LOG_INFO("pid: %lu p_gsn: %lu", node.PageID(), node_ptr->p_gsn);
  node.Unlock();
  return true;
}

bool BTree::ReadPID(pageid_t pid) {
  GuardS<BTreeNode> node(buffer_, pid);
  buffer_->ReadPID(pid);
  node.Unlock();
  return true;
}

/**
 * @brief Only used for Blob State indexes.
 * Similar to LookUp operator, but for Byte String as key
 */
auto BTree::LookUpBlob(std::span<const u8> blob_key,
                       const ComparisonLambda &cmp, const PayloadFunc &read_cb)
    -> bool {
  Ensure(cmp_lambda_.op == ComparisonOperator::BLOB_STATE);
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  leng_t unused;
  auto search_key = blob::BlobLookupKey(blob_key);

  while (true) {
    try {
      GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);
      while (node->IsInner()) {
        node = GuardO<BTreeNode>(
            buffer_, node->FindChildWithBlobKey(search_key, unused, cmp), node);
      }

      bool found;
      leng_t pos = node->LowerBoundWithBlobKey(search_key, found, cmp);
      if (!found) {
        return false;
      }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {
    }
  }
}

} // namespace leanstore::storage