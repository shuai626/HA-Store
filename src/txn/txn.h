#ifndef _TXN_H_
#define _TXN_H_

#include <map>
#include <set>
#include <vector>
#include <deque>

#include "utils/static_thread_pool.h"
#include "utils/common.h"
#include "utils/mutex.h"

using std::map;
using std::set;
using std::vector;
using std::deque;

// Txns can have five distinct status values:
enum TxnStatus
{
    INCOMPLETE  = 0,  // Not yet executed
    COMPLETED_C = 1,  // Executed (with commit vote)
    COMPLETED_A = 2,  // Executed (with abort vote)
    COMMITTED   = 3,  // Committed
    ABORTED     = 4,  // Aborted
};

class Txn
{
   public:
    // Commit vote defauls to false. Only by calling "commit"
    Txn() : status_(INCOMPLETE) {}
    virtual ~Txn() {}
    virtual Txn* clone() const = 0;  // Virtual constructor (copying)

    // Method containing all the transaction's method logic.
    virtual void Run() = 0;
    
    // Returns the Txn's current execution status.
    TxnStatus Status() { return status_; }
    // Checks for overlap in read and write sets. If any key appears in both,
    // an error occurs.
    void CheckReadWriteSets();

   protected:
    // Copies the internals of this txn into a given transaction (i.e.
    // the readset, writeset, and so forth).  Be sure to modify this method
    // to copy any new data structures you create.
    void CopyTxnInternals(Txn* txn) const;

    friend class TxnProcessor;

    // Method to be used inside 'Execute()' function when reading records from
    // the database. If record corresponding with specified 'key' exists, sets
    // '*value' equal to the record value and returns true, else returns false.
    //
    // Requires: key appears in readset or writeset
    //
    // Note: Can ONLY be called from inside the 'Execute()' function.
    bool Read(const Key& key, Value* value);

    // Method to be used inside 'Execute()' function when writing records to
    // the database.
    //
    // Requires: key appears in writeset
    //
    // Note: Can ONLY be called from inside the 'Execute()' function.
    void Write(const Key& key, const Value& value);

// Macro to be used inside 'Execute()' function when deciding to COMMIT.
//
// Note: Can ONLY be called from inside the 'Execute()' function.
#define COMMIT                 \
    do                         \
    {                          \
        status_ = COMPLETED_C; \
        return;                \
    } while (0)

// Macro to be used inside 'Execute()' function when deciding to ABORT.
//
// Note: Can ONLY be called from inside the 'Execute()' function.
#define ABORT                  \
    do                         \
    {                          \
        status_ = COMPLETED_A; \
        return;                \
    } while (0)

    // Set of all keys that may need to be read in order to execute the
    // transaction.
    set<Key> readset_;

    // Set of all keys that may be updated when executing the transaction.
    set<Key> writeset_;

    // Results of reads performed by the transaction.
    map<Key, Value> reads_;

    // Key, Value pairs WRITTEN by the transaction.
    map<Key, Value> writes_;

    // Transaction's current execution status.
    TxnStatus status_;

    // Unique, monotonically increasing transaction ID, assigned by TxnProcessor.
    uint64 unique_id_;

    // Start index (used for OCC).
    int64_t occ_start_idx_;

    // Timestamp (used for H-Store).
    time_t hstore_start_time_;

    // Cond and wait structures (used for H-Store).
    pthread_mutex_t hstore_subplan_mutex_;
    pthread_cond_t h_store_subplan_cond_;

    pthread_mutex_t hstore_commit_abort_mutex_;
    pthread_cond_t hstore_commit_abort_cond_;

    // H-Store partition threads that have yet to respond back to Command Router
    set<StaticThreadPool*> hstore_pending_partition_threads_;

    // Flag for txn class type: false = single-site/one-shot/sterile, true = multipartition
    volatile bool hstore_is_multipartition_transaction_;

    // Flag that checks if any partition thread aborted a multipartition transaction
    volatile bool hstore_is_aborted_;
    
    volatile bool hstore_is_first_phase_multitxn_;
    
    volatile bool hstore_commit_abort_;

    Mutex mutex_;
};

#endif  // _TXN_H_
