#ifndef _TXN_PROCESSOR_H_
#define _TXN_PROCESSOR_H_

#include <deque>
#include <map>
#include <string>

#include "lock_manager.h"
#include "mvcc_storage.h"
#include "storage.h"
#include "txn.h"
#include "utils/atomic.h"
#include "utils/common.h"
#include "utils/mutex.h"
#include "utils/static_thread_pool.h"

using std::deque;
using std::map;
using std::string;

// The TxnProcessor supports five different execution modes, corresponding to
// the four parts of assignment 2, plus a simple serial (non-concurrent) mode.
enum CCMode
{
    SERIAL                 = 0,  // Serial transaction execution (no concurrency)
    LOCKING_EXCLUSIVE_ONLY = 1,  // Part 1A
    LOCKING                = 2,  // Part 1B
    OCC                    = 3,  // Part 2
    P_OCC                  = 4,  // Part 3
    MVCC                   = 5,  // Part 4
    
    H_STORE                = 6,  // Final Project
};

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode);

class TxnProcessor
{
   public:
    // The TxnProcessor's constructor starts the TxnProcessor running in the
    // background.
    explicit TxnProcessor(CCMode mode, int dbsize, int partition_thread_count);

    // The TxnProcessor's destructor stops all background threads and deallocates
    // all objects currently owned by the TxnProcessor, except for Txn objects.
    ~TxnProcessor();

    // Registers a new txn request to be executed by the TxnProcessor.
    // Ownership of '*txn' is transfered to the TxnProcessor.
    void NewTxnRequest(Txn* txn);

    // Returns a pointer to the next COMMITTED or ABORTED Txn. The caller takes
    // ownership of the returned Txn.
    Txn* GetTxnResult();

    // Main loop implementing all concurrency control/thread scheduling.
    void RunScheduler();

    static void* StartScheduler(void* arg);

   private:
    // Serial validation
    bool SerialValidate(Txn* txn);

    // Parallel executtion/validation for OCC
    void ExecuteTxnParallel(Txn* txn);

    // Serial version of scheduler.
    void RunSerialScheduler();

    // Locking version of scheduler.
    void RunLockingScheduler();

    // OCC version of scheduler.
    void RunOCCScheduler();

    // OCC version of scheduler with parallel validation.
    void RunOCCParallelScheduler();

    // MVCC version of scheduler.
    void RunMVCCScheduler();

    // H-Store version of scheduler.
    void RunHStoreScheduler();

    // Performs all reads required to execute the transaction, then executes the
    // transaction logic.
    void ExecuteTxn(Txn* txn);

    // Applies all writes performed by '*txn' to 'storage_'.
    //
    // Requires: txn->Status() is COMPLETED_C.
    void ApplyWrites(Txn* txn);

    // The following functions are for MVCC
    void MVCCExecuteTxn(Txn* txn);

    bool MVCCCheckWrites(Txn* txn);

    void MVCCLockWriteKeys(Txn* txn);

    void MVCCUnlockWriteKeys(Txn* txn);

    // The logic in this returns the threadpool (single thread) for the correct partition given a key
    StaticThreadPool* GetPartitionThreadPool(Key key);

    // The following functions are for H-Store
    void HStoreExecuteTxn(Txn* txn);

    void HStorePartitionThreadExecuteTxn(Txn* txn, int partition);

    void GarbageCollection();

    // Concurrency control mechanism the TxnProcessor is currently using.
    CCMode mode_;

    // Thread pool managing all threads used by TxnProcessor.
    StaticThreadPool tp_;

    // Data storage used for all modes.
    Storage* storage_;

    // Next valid unique_id, and a mutex to guard incoming txn requests.
    int next_unique_id_;
    Mutex mutex_;

    // Queue of incoming transaction requests.
    AtomicQueue<Txn*> txn_requests_;

    // Queue of txns that have acquired all locks and are ready to be executed.
    //
    // Does not need to be atomic because RunScheduler is the only thread that
    // will ever access this queue.
    deque<Txn*> ready_txns_;

    // Queue of completed (but not yet committed/aborted) transactions.
    AtomicQueue<Txn*> completed_txns_;

    // Vector of committed transactions that are used to check any overlap
    // during OCC validation phase.
    AtomicVector<Txn*> committed_txns_;

    // Queue of transaction results (already committed or aborted) to be returned
    // to client.
    AtomicQueue<Txn*> txn_results_;

    // Set of transactions that are currently in the process of parallel
    // validation.
    AtomicSet<Txn*> active_set_;

    // Used it for critical section in parallel occ.
    Mutex active_set_mutex_;

    // Lock Manager used for LOCKING concurrency implementations.
    LockManager* lm_;

    // Used for stopping the continuous loop that runs in the scheduler thread
    bool stopped_;

    // Gives us access to the scheduler thread so that we can wait for it to join later.
    pthread_t scheduler_thread_;

    // Partition thread used for H_STORE concurrency implementation
    // Each partition thread is implemented as a static thread pool of size 1
    deque<StaticThreadPool*> partition_threads_;

    // Strategy flag used for H_STORE concurrency implementation
    // 0 = Basic, 1 = Intermediate, 2 = Advanced
    volatile int strategy_;

    // Abort count to determine strategy for H_STORE concurrency implementation
    int abort_count_;

    // Size of the database for the current test.
    int dbsize_;

    // Number of partition threads
    int partition_thread_count_;
};

#endif  // _TXN_PROCESSOR_H_
