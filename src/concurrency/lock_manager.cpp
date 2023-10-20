//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>

#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 先检查加锁的有效性
  BUSTUB_ASSERT(txn->GetState() != TransactionState::ABORTED && txn->GetState() != TransactionState::COMMITTED,
                "状态异常");
  AbortReason reason;
  if (!ValidityOfLock(lock_mode, txn, true, oid, RID{}, reason)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  std::unique_lock<std::mutex> lock(table_lock_map_latch_);

  // 返回资源的加锁队列，若队列不存在则创建队列,若是新创建的队列，则无需检查锁升级

  std::shared_ptr<LockRequestQueue> queue;
  auto queue_is_exist = static_cast<bool>(table_lock_map_.count(oid));
  if (!queue_is_exist) {
    // 创建队列，将锁请求放入

    auto request_queue = std::make_shared<LockRequestQueue>();
    auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    (request_queue->request_queue_).push_back(new_request.get());
    table_lock_map_.insert({oid, request_queue});
    queue = request_queue;
    lock.unlock();

  } else {
    // 请求队列存在则检查是否升级
    // 为了提高并发性，在获取到 queue 之后立即释放 map 的锁
    auto request_queue = table_lock_map_.at(oid);
    // 疑问，如果在获取队列后对资源操作的阶段，另一个线程会释放当前的资源的锁吗？2pl？？
    // 想了下不太可能，加锁和解锁都是一个事务放出来的请求，肯定是操作结束后才会释放锁，并从队列中删去
    lock.unlock();
    auto upgrade_result = LockUpgradeandInsert(lock_mode, txn, true, oid, RID{}, request_queue, reason);
    queue = request_queue;
    if (!upgrade_result) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), reason);
    }
  }

  // 在对应资源的请求队列末尾添加一条记录
  // auto lock_queue = std::make_shared<LockRequestQueue>(table_lock_map_[oid].get());

  std::unique_lock<std::mutex> queuelock(queue->latch_);

  while (!GrantCompatibleLock(lock_mode, txn, queue, true)) {
    queue->cv_.wait(queuelock);
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 检查行锁是否释放完毕

  std::unique_lock<std::mutex> lock(table_lock_map_latch_);

  auto queue = table_lock_map_.at(oid);

  std::unique_lock<std::mutex> queuelock(queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto tmp : queue->request_queue_) {
    if (txn->GetTransactionId() == tmp->txn_id_ && tmp->granted_) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_COMMITTED:
          if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (tmp->lock_mode_ == LockMode::SHARED || tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_UNCOMMITTED:
          if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          BUSTUB_ASSERT(tmp->lock_mode_ != LockMode::SHARED, "不允许使用s锁");
          break;
      }
      // 删除表锁集合
      UpdateTableLockSet(txn, oid, tmp->lock_mode_, false);
      queue->request_queue_.remove(tmp);
      delete tmp;

      queue->cv_.notify_all();
      queuelock.unlock();
      break;
    }
    if (tmp == queue->request_queue_.back()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  AbortReason reason;

  // 能否不加锁直接获取表锁列表？？感觉可以
  // 一下代码用于检查 是否已获取表锁
  auto table_queue = table_lock_map_.at(oid)->request_queue_;
  for (auto tmp : table_queue) {
    if (tmp->txn_id_ == txn->GetTransactionId() && tmp->granted_) {
      break;
    }
    if (tmp == table_queue.back()) {
      // 遍历到最后一个也没找到表锁，抛出异常
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (!ValidityOfLock(lock_mode, txn, true, oid, rid, reason)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  std::unique_lock<std::mutex> lock(row_lock_map_latch_);

  // 返回资源的加锁队列，若队列不存在则创建队列,若是新创建的队列，则无需检查锁升级

  std::shared_ptr<LockRequestQueue> queue;
  auto queue_is_exist = static_cast<bool>(row_lock_map_.count(rid));
  if (!queue_is_exist) {
    // 创建队列和请求，将锁请求放入队列

    auto request_queue = std::make_shared<LockRequestQueue>();
    auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    (request_queue->request_queue_).push_back(new_request.get());
    row_lock_map_.insert({rid, request_queue});
    queue = request_queue;
    std::unique_lock<std::mutex> unlock(row_lock_map_latch_);

  } else {
    // 请求队列存在则检查是否升级
    // 为了提高并发性，在获取到 queue 之后立即释放 map 的锁
    auto request_queue = row_lock_map_.at(rid);
    // 疑问，如果在获取队列后对资源操作的阶段，另一个线程会释放当前的资源的锁吗？2pl？？
    // 想了下不太可能，加锁和解锁都是一个事务放出来的请求，肯定是操作结束后才会释放锁，并从队列中删去
    std::unique_lock<std::mutex> unlock(row_lock_map_latch_);
    auto upgrade_result = LockUpgradeandInsert(lock_mode, txn, true, oid, RID{}, request_queue, reason);
    queue = request_queue;
    if (!upgrade_result) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), reason);
    }
  }

  // 在对应资源的请求队列末尾添加一条记录
  // auto lock_queue = std::make_shared<LockRequestQueue>(table_lock_map_[oid].get());

  std::unique_lock<std::mutex> queuelock(queue->latch_);

  while (!GrantCompatibleLock(lock_mode, txn, queue, false)) {
    queue->cv_.wait(queuelock);
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(row_lock_map_latch_);

  auto queue = row_lock_map_.at(rid);

  std::unique_lock<std::mutex> queuelock(queue->latch_);
  row_lock_map_latch_.unlock();

  for (auto tmp : queue->request_queue_) {
    if (txn->GetTransactionId() == tmp->txn_id_ && tmp->granted_) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_COMMITTED:
          if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (tmp->lock_mode_ == LockMode::SHARED || tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_UNCOMMITTED:
          if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          BUSTUB_ASSERT(tmp->lock_mode_ != LockMode::SHARED, "不允许使用s锁");
          break;
      }
      // 删除表锁集合
      UpdateRowLockSet(txn, oid, rid, tmp->lock_mode_, false);
      queue->request_queue_.remove(tmp);
      delete tmp;

      queue->cv_.notify_all();
      queuelock.unlock();
      break;
    }
    if (tmp == queue->request_queue_.back()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  }
  return true;

  return true;
}

auto LockManager::ValidityOfLock(LockMode mode, Transaction *txn, bool table_lock, table_oid_t oid, RID rid,
                                 AbortReason &reason) -> bool {
  /*判断当前类型 锁模式是否支持*/
  if (!table_lock) {
    // 表锁支持所有锁
    if (mode != LockMode::EXCLUSIVE && mode != LockMode::SHARED) {
      reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
      return false;
    }
  }
  /*判断隔离级别是否支持*/

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // shrinking 阶段不可使用任何锁

    if (txn->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }

  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        (mode != LockMode::INTENTION_SHARED && mode != LockMode::SHARED)) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }

  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (mode != LockMode::EXCLUSIVE && mode != LockMode::INTENTION_EXCLUSIVE) {
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }

    if (txn->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }
  /*多层级锁，保证在行锁前已经上了表锁,*/
  if (!table_lock) {
    if (mode == LockMode::SHARED) {
      if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
          !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else if (mode == LockMode::EXCLUSIVE) {
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
    /*锁升级要保证有效*/
  }
  return true;
}

auto LockManager::LockUpgradeandInsert(LockMode mode, Transaction *txn, bool table_lock, table_oid_t oid, RID rid,
                                       std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason) -> bool {
  // 升级请求 只能是相对已上锁的资源来说的，若资源未上锁那么就谈不上需要升级
  // 因此需要在已上锁的资源上进行升级，可以从事务维护的锁集合中找到.（发现不现实，因为你不知道事务之前上的锁在哪个队列）
  std::unique_lock<std::mutex> lock(queue->latch_);
  auto iterator = queue->request_queue_.begin();
  for (auto tmp : queue->request_queue_) {
    if (tmp->txn_id_ == txn->GetTransactionId()) {
      if (tmp->granted_) {
        // 尝试进行锁升级
        // 当前加锁是否重复？
        if (tmp->lock_mode_ == mode) {
          return true;
        }
        // 检查是否有锁在升级
        if (queue->upgrading_ != INVALID_TXN_ID) {
          // 已经有锁在升级，抛出异常
          // 不允许同时锁升级是为了保证之前的锁升级优先完成，也就是使得锁升级的过程是串行执行。
          // 若之前有锁升级，并且未完成，就会导致之后的锁升级插队到的等待获取锁的队列中的前面，违反了先进先出
          reason = AbortReason::UPGRADE_CONFLICT;
          return false;
          // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
        // 判断锁升级类型是否有效
        switch (tmp->lock_mode_) {
          case LockMode::SHARED:
            if (mode != LockMode::EXCLUSIVE && mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
              reason = AbortReason::INCOMPATIBLE_UPGRADE;
              return false;
            }
            break;
          case LockMode::INTENTION_SHARED:
            if (mode == LockMode::SHARED) {
              reason = AbortReason::INCOMPATIBLE_UPGRADE;
              return false;
            }
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            if (mode != LockMode::EXCLUSIVE) {
              reason = AbortReason::INCOMPATIBLE_UPGRADE;
              return false;
            }
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            if (mode != LockMode::EXCLUSIVE && mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
              reason = AbortReason::INCOMPATIBLE_UPGRADE;
              return false;
            }
            break;
          case LockMode::EXCLUSIVE:
            reason = AbortReason::INCOMPATIBLE_UPGRADE;
            return false;
            break;
        }
        // 释放锁后new一个新锁加入队列
        queue->upgrading_ = txn->GetTransactionId();
        // 要从表锁或者行锁集合中删除释放的锁.
        if (table_lock) {
          switch (tmp->lock_mode_) {
            case LockMode::SHARED:
              txn->GetSharedTableLockSet()->erase(tmp->oid_);
              break;
            case LockMode::INTENTION_SHARED:
              txn->GetIntentionSharedTableLockSet()->erase(tmp->oid_);
              break;
            case LockMode::SHARED_INTENTION_EXCLUSIVE:
              txn->GetSharedIntentionExclusiveTableLockSet()->erase(tmp->oid_);
              break;
            case LockMode::INTENTION_EXCLUSIVE:
              txn->GetIntentionExclusiveTableLockSet()->erase(tmp->oid_);
              break;
            case LockMode::EXCLUSIVE:
              txn->GetExclusiveTableLockSet()->erase(tmp->oid_);
              break;
          }

          queue->request_queue_.remove(tmp);
          assert(oid == tmp->oid_);
          delete tmp;
          auto upgrade_quest = std::make_shared<LockRequest>(txn->GetTransactionId(), mode, oid);
          queue->request_queue_.insert(iterator, upgrade_quest.get());
          queue->cv_.notify_all();                             // 存疑？要通知吗
          std::unique_lock<std::mutex> unlock(queue->latch_);  // 可以自动释放
        } else {
          // 是行锁

          UpdateRowLockSet(txn, oid, rid, mode, false);  // 删除行锁集合
          queue->request_queue_.remove(tmp);
          delete tmp;
          auto upgrade_quest = std::make_shared<LockRequest>(txn->GetTransactionId(), mode, oid, rid);
          queue->request_queue_.insert(iterator, upgrade_quest.get());
          queue->cv_.notify_all();                             // 存疑？要通知吗
          std::unique_lock<std::mutex> unlock(queue->latch_);  // 可以自动释放
        }
        return true;
      }
    }
    iterator++;
  }
  // 假如遍历队列后发现不存在与当前 tid 相同的请求，代表这是一次平凡的锁请求。
  // 那么将锁请求加入队列末尾
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), mode, oid);
  (queue->request_queue_).push_back(new_request.get());
  std::unique_lock<std::mutex> unlock(queue->latch_);
  return true;
}

void LockManager::CompatibleLock(const std::unordered_set<LockMode> &granted_lock,
                                 std::unordered_set<LockMode> &compatible_lock) {
  // 初始化可兼容序列
  // compatible_lock =
  //     std::unordered_set<LockMode>{LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE,
  //                                  LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_SHARED,
  //                                  LockMode::SHARED};
  // 依据已经加的锁排除掉不能加的锁
  for (auto mode : granted_lock) {
    switch (mode) {
      case LockMode::EXCLUSIVE:
        compatible_lock.clear();
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        compatible_lock.clear();
        compatible_lock.insert(LockMode::INTENTION_SHARED);
        break;
      case LockMode::SHARED:
        compatible_lock.erase(LockMode::INTENTION_EXCLUSIVE);
        compatible_lock.erase(LockMode::SHARED_INTENTION_EXCLUSIVE);
        compatible_lock.erase(LockMode::EXCLUSIVE);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        compatible_lock.erase(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE);
        compatible_lock.erase(LockManager::LockMode::SHARED);
        compatible_lock.erase(LockManager::LockMode::EXCLUSIVE);
        break;
      case LockMode::INTENTION_SHARED:
        compatible_lock.erase(LockManager::LockMode::EXCLUSIVE);
        break;
    }
  }
}

auto LockManager::GrantCompatibleLock(LockMode mode, Transaction *txn, std::shared_ptr<LockRequestQueue> &queue,
                                      bool table_lock) -> bool {
  // 当前是新创建的锁请求队列，特殊处理
  if (queue->request_queue_.size() == 1) {
    // 将 锁加入集合
    auto iterator = queue->request_queue_.begin();
    if (table_lock) {
      UpdateTableLockSet(txn, (*iterator)->oid_, (*iterator)->lock_mode_, true);
    } else {
      UpdateRowLockSet(txn, (*iterator)->oid_, (*iterator)->rid_, (*iterator)->lock_mode_, true);
    }
    return true;
  }
  std::unordered_set<LockMode> granted_lock{};  // granted 是当前已有的锁，用来判断还能加的锁范围。
  std::unordered_set<LockMode> compatible_lock{LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE,
                                               LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::INTENTION_SHARED,
                                               LockMode::SHARED};
  auto iterator = queue->request_queue_.begin();  // 迭代器会指向第一个没授予grant的请求
  // 找到当前已上锁的请求，将其加入granted_lock集合
  for (auto tmp : queue->request_queue_) {
    if (!tmp->granted_) {
      break;
    }
    granted_lock.insert(tmp->lock_mode_);
    iterator++;
  }
  CompatibleLock(granted_lock, compatible_lock);
  // 若没有兼容的锁则返回false
  if (compatible_lock.empty()) {
    return false;
  }
  // 若有能兼容的锁，则判断 事务请求的锁类型是否能获取

  for (auto it = iterator; it != queue->request_queue_.end(); it++) {
    auto next_grant_lockmode = (*it)->lock_mode_;
    auto number_to_be_granted = 0;
    if (compatible_lock.count(next_grant_lockmode) == 1) {
      number_to_be_granted++;
      // BUSTUB_ASSERT((*it)->granted_ == false, "已授予锁权限,不应该，迭代器逻辑出现问题");
      granted_lock.insert(next_grant_lockmode);
      CompatibleLock(granted_lock, compatible_lock);
    } else {
      // 循环遍历，一次性授予锁
      for (auto i = 0; i < number_to_be_granted; i++) {
        if (queue->upgrading_ != INVALID_TXN_ID && i == 0) {
          queue->upgrading_ = INVALID_TXN_ID;
        }
        // 依次授予锁
        BUSTUB_ASSERT((*iterator)->granted_ == false, "对已授权的锁再次授予锁");
        (*iterator)->granted_ = true;
        // 维护txn中的锁集合
        if (table_lock) {
          UpdateTableLockSet(txn, (*iterator)->oid_, (*iterator)->lock_mode_, true);
        } else {
          UpdateRowLockSet(txn, (*iterator)->oid_, (*iterator)->rid_, (*iterator)->lock_mode_, true);
        }
        iterator++;
      }
      // 当compatible_lock.count(next_grant_lockmode) ==0
      // 时，一次性授予锁，然后外循环结束，不再向后寻找能支持的锁，这是为了满足fifo
      break;
    }
  }

  return true;
}

void LockManager::UpdateTableLockSet(Transaction *txn, table_oid_t oid, LockMode mode, bool is_insert) {
  // 根据锁模式、插入还是删除做区分
  switch (mode) {
    case LockMode::SHARED:
      if (is_insert) {
        txn->GetSharedTableLockSet()->insert(oid);
      } else {
        txn->GetSharedTableLockSet()->erase(oid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (is_insert) {
        txn->GetExclusiveTableLockSet()->insert(oid);
      } else {
        txn->GetExclusiveTableLockSet()->erase(oid);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (is_insert) {
        txn->GetIntentionSharedTableLockSet()->insert(oid);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(oid);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (is_insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (is_insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      }
      break;
  }
}

void LockManager::UpdateRowLockSet(Transaction *txn, table_oid_t oid, RID rid, LockMode mode, bool is_insert) {
  // 根据锁模式、插入还是删除做区分
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  switch (mode) {
    case LockMode::SHARED:
      if (is_insert) {
        InsertRowLockSet(s_row_lock_set, oid, rid);
      } else {
        DeleteRowLockSet(s_row_lock_set, oid, rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (is_insert) {
        InsertRowLockSet(x_row_lock_set, oid, rid);
      } else {
        DeleteRowLockSet(x_row_lock_set, oid, rid);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].push_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iterator = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iterator != waits_for_[t1].end()) {
    waits_for_[t1].erase(iterator);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 假设图已被构建好；
  //  visited 用来放等待资源的事务id。visited中的值不会发生重复，在插入前会判断是否成环
  std::set<txn_id_t> visited;
  for (const auto &[start_node, end_node_set] : waits_for_) {
    if (visited.find(start_node) == visited.end()) {
      auto abort_id = DepthFirstSearch(start_node, visited);
      if (abort_id != NO_CYCLE) {
        *txn_id = abort_id;
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
       // 构建图前要对请求队列加锁
      std::unique_lock table_lock(table_lock_map_latch_);
      std::unique_lock row_lock(row_lock_map_latch_);
      LockManager::RebuildWaitForGraph();
      table_lock.unlock();
      row_lock.unlock();
      txn_id_t abort_txn = NO_CYCLE;

      while (LockManager::HasCycle(&abort_txn)) {
        // 当我们决定中止事务时，修剪已经构建的等待图
        waits_for_.erase(abort_txn);
        for (auto &[start_node, edge_vector] : waits_for_) {
          auto it = std::find(edge_vector.begin(), edge_vector.end(), abort_txn);
          edge_vector.erase(it);
        }
        // 将此事务设置为中止
        auto abort_ptr = TransactionManager::GetTransaction(abort_txn);
        abort_ptr->SetState(TransactionState::ABORTED);
      }
      if (abort_txn != NO_CYCLE) {
        // 如果我们发现要终止一个周期，通知所有线程
        LockManager::NotifyAllTransaction();
      }
    }
  }
}

}  // namespace bustub
