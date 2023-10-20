//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

#define NO_CYCLE (-1)  // 定义一个宏，表示没有环

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), table_lock_(true) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid), table_lock_(false) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /**用于判断是表锁还是行锁**/
    bool table_lock_;

    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<LockRequest *> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;  // 一个用于通知在这个请求队列上被阻塞的事务的线程同步机制。
    // 当请求队列中有一个新的请求或者请求已经被处理时，这个条件变量将被触发。
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */  // 保证只有一个线程对锁队列进行操作
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*检测*/
  /**
   *依据传入的锁模式、事务的阶段、隔离等级，表锁还是行锁来判断加锁是否合法
   */
  auto ValidityOfLock(LockMode mode, Transaction *txn, bool table_lock, table_oid_t oid, RID rid, AbortReason &reason)
      -> bool;

  /**
   *检查是否是一次锁升级请求，若是锁升级，则需要在队列中erase锁，并在相应位置new一个新的request插入
   *发生锁升级时，一定是之前请求过的事务再次请求锁，而且这个事务在当前一定被授予了一个锁
   *原因在于，若一个事务未被授权，那么他应该在请求队列中被阻塞，自然不可能发出第二次加锁请求
   *若这个事务第一次申请锁，那就不可能进入锁升级。
   **若不是锁升级，则在队列末尾插入一个请求。
   */
  auto LockUpgradeandInsert(LockMode mode, Transaction *txn, bool table_lock, table_oid_t oid, RID rid,
                            std::shared_ptr<LockRequestQueue> &queue, AbortReason &reason) -> bool;

  /**
   *兼容锁的判断逻辑
   *找到grant的锁
   *判断当前已grant的锁还能支持什么类型的锁
   *判断第一个未授予的锁满足是否满足上一步的支持类型，若支持则加入，否则false
   *循环判断当前grant的锁支持什么类型
   *
   *要有一个接口能根据已授权的锁 输出还能兼容的锁类型
   */
  auto GrantCompatibleLock(LockMode mode, Transaction *txn, std::shared_ptr<LockRequestQueue> &queue, bool table_lock)
      -> bool;

  void CompatibleLock(const std::unordered_set<LockMode> &granted_lock, std::unordered_set<LockMode> &compatible_lock);

  void UpdateTableLockSet(Transaction *txn, table_oid_t oid, LockMode mode, bool is_insert);

  void UpdateRowLockSet(Transaction *txn, table_oid_t oid, RID rid, LockMode mode, bool is_insert);

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  auto InsertRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      lock_set->emplace(oid, std::unordered_set<RID>{});
      row_lock_set = lock_set->find(oid);
    }
    row_lock_set->second.emplace(rid);
  }

  auto DeleteRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      return;
    }
    row_lock_set->second.erase(rid);
  }

 private:
  /** Fall 2022 */

  auto DepthFirstSearch(txn_id_t curr_txn, std::set<txn_id_t> &visited) -> txn_id_t {
    if (waits_for_.find(curr_txn) != waits_for_.end()) {  // dfs的遍历条件，waitfor中能找到当前“node”所在的请求队列
      visited.insert(curr_txn);                           // 只保留发生了 等待资源的 事务
      for (const auto &neighbor : waits_for_[curr_txn]) {
        if (visited.find(neighbor) == visited.end()) {  // 没找到相同节点则可以往下遍历,有相同节点时，直接传出txnid
          auto abort_id = DepthFirstSearch(neighbor, visited);
          if (abort_id != NO_CYCLE) {  // 发现环了，将,要终止的txnid传出,发现环后层级return，停止遍历
            return abort_id;
          }
        } else {
          return neighbor;
          // 记录当前成环的txn
        }
      }
    }
    return NO_CYCLE;
  }

  void NotifyAllTransaction() {
    for (const auto &[table_id, request_queue] : table_lock_map_) {
      request_queue->latch_.lock();
      request_queue->cv_.notify_all();
      request_queue->latch_.unlock();
    }
    for (const auto &[row_id, request_queue] : row_lock_map_) {
      request_queue->latch_.lock();
      request_queue->cv_.notify_all();
      request_queue->latch_.unlock();
    }
  }

  /* 遍历 table_lock_map 和 row_lock_map 中所有的请求队列，对于
   * 每一个请求队列，用一个二重循环将所有满足等待关系的一对 tid
   * 加入 wait for 图的边集*/
  void RebuildWaitForGraph() {
    waits_for_.clear();
    for (const auto &[table_id, request_queue] : table_lock_map_) {
      request_queue->latch_.lock();
      std::set<txn_id_t> granted;
      for (const auto &request : request_queue->request_queue_) {
        if (request->granted_) {
          granted.insert(request->txn_id_);
        } else {
          // waits for a resource, build an edge
          for (const auto &holder : granted) {
            AddEdge(request->txn_id_, holder);
          }
        }
      }
      request_queue->latch_.unlock();
    }
    for (const auto &[row_id, request_queue] : row_lock_map_) {
      request_queue->latch_.lock();
      std::set<txn_id_t> granted;
      for (const auto &request : request_queue->request_queue_) {
        if (request->granted_) {
          granted.insert(request->txn_id_);
        } else {
          for (const auto &holder : granted) {
            AddEdge(request->txn_id_, holder);
          }
        }
      }
      request_queue->latch_.unlock();
    }
    // 将图的边有序化，使最老的事务优先被dfs访问。这样最young的事务会最后被访问，可以直接弹出
    for (auto &[node, edge] : waits_for_) {
      std::sort(edge.begin(), edge.end());
    }
  }

  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub
