#include "txn_processor.h"
#include <stdio.h>
#include <set>
#include <unordered_set>

#include "lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

// Number of partitions when using H-STORE
#define PARTITION_THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode, int dbsize) : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1)
{
    if (mode_ == LOCKING_EXCLUSIVE_ONLY)
        lm_ = new LockManagerA(&ready_txns_);
    else if (mode_ == LOCKING)
        lm_ = new LockManagerB(&ready_txns_);
    

    /* TODO: If mode_ == H_STORE, then "partition the database" and spawns StaticThreadPool of 1 thread for each partition.
       Store partition threads inside partition_threads_ defined in txn_processor.h */
    if (mode_ == H_STORE)
    {
        strategy_ = 0;
        abort_count_ = 0;
        dbsize_ = dbsize;

        /* TODO: Implement partitioning here. In our implementation, it is sufficient to
         initiliaze partition_threads_ with <# processor> threads and store the database size or partition section size (database size / <# processor>). 
         We will assume the database is partitioned into <# processor> contiguous blocks.  */

        for (int i = 0; i < PARTITION_THREAD_COUNT; i++) 
        {
            StaticThreadPool next(1);
            partition_threads_.PushValue(next);
        }

    }

    // Create the storage
    if (mode_ == MVCC)
    {
        storage_ = new MVCCStorage();
    }
    else
    {
        storage_ = new Storage();
    }

    storage_->InitStorage();

    // Start 'RunScheduler()' running.

    pthread_attr_t attr;
    pthread_attr_init(&attr);

#if !defined(_MSC_VER) && !defined(__APPLE__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < 7; i++)
    {
        CPU_SET(i, &cpuset);
    }
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
#endif

    pthread_t scheduler_;
    pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

    stopped_          = false;
    scheduler_thread_ = scheduler_;
}

void* TxnProcessor::StartScheduler(void* arg)
{
    reinterpret_cast<TxnProcessor*>(arg)->RunScheduler();
    return NULL;
}

TxnProcessor::~TxnProcessor()
{
    // Wait for the scheduler thread to join back before destroying the object and its thread pool.
    stopped_ = true;
    pthread_join(scheduler_thread_, NULL);

    if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING) delete lm_;

    delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn)
{
    // Atomically assign the txn a new number and add it to the incoming txn
    // requests queue.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult()
{
    Txn* txn;
    while (!txn_results_.Pop(&txn))
    {
        // No result yet. Wait a bit before trying again (to reduce contention on
        // atomic queues).
        usleep(1);
    }
    return txn;
}

void TxnProcessor::RunScheduler()
{
    switch (mode_)
    {
        case SERIAL:
            RunSerialScheduler();
            break;
        case LOCKING:
            RunLockingScheduler();
            break;
        case LOCKING_EXCLUSIVE_ONLY:
            RunLockingScheduler();
            break;
        case OCC:
            RunOCCScheduler();
            break;
        case P_OCC:
            RunOCCParallelScheduler();
            break;
        case MVCC:
            RunMVCCScheduler();
        case H_STORE:
            RunHStoreScheduler();
    }
}

void TxnProcessor::RunSerialScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Get next txn request.
        if (txn_requests_.Pop(&txn))
        {
            // Execute txn.
            ExecuteTxn(txn);

            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }

            // Return result to client.
            txn_results_.Push(txn);
        }
    }
}

void TxnProcessor::RunLockingScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            bool blocked = false;
            // Request read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                if (!lm_->ReadLock(txn, *it))
                {
                    blocked = true;
                }
            }

            // Request write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                if (!lm_->WriteLock(txn, *it))
                {
                    blocked = true;
                }
            }

            // If all read and write locks were immediately acquired, this txn is
            // ready to be executed.
            if (blocked == false)
            {
                ready_txns_.push_back(txn);
            }
        }

        // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn))
        {
            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }

            // Release read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
            // Release write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }

            // Return result to client.
            txn_results_.Push(txn);
        }

        // Start executing all transactions that have newly acquired all their
        // locks.
        while (ready_txns_.size())
        {
            // Get next ready txn from the queue.
            txn = ready_txns_.front();
            ready_txns_.pop_front();

            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->ExecuteTxn(txn); });
        }
    }
}

void TxnProcessor::ExecuteTxn(Txn* txn)
{
    // Get the current commited transaction index for the further validation.
    txn->occ_start_idx_ = committed_txns_.Size();

    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();

    // Hand the txn back to the RunScheduler thread.
    completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn)
{
    // Write buffered writes out to storage.
    for (map<Key, Value>::iterator it = txn->writes_.begin(); it != txn->writes_.end(); ++it)
    {
        storage_->Write(it->first, it->second, txn->unique_id_);
    }
}

void TxnProcessor::RunOCCScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Get the next new transaction request (if one is pending) and pass it to an execution thread.
        if (txn_requests_.Pop(&txn))
        {
            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->ExecuteTxn(txn); });
        }

        // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn))
        {
            int occ_end_idx = committed_txns_.Size();
            bool valid = true;

            int i ;
            // Validation phase:
            // Check overlap with each record whose key appears in the txn's read and write sets
            for (i = txn->occ_start_idx_; i < occ_end_idx; i++)
            {
                Txn* commit_txn = committed_txns_[i];

                for (Key write_key : commit_txn->writeset_)
                {
                    if (txn->readset_.count(write_key) > 0)
                    {
                        valid = false;
                    }
                }
            }

            if (!valid)
            {
                // Clean-up transaction
                txn->reads_.clear();
                txn->writes_.clear();
                txn->status_ = INCOMPLETE;

                // Restart transaction
                mutex_.Lock();
                txn->unique_id_ = next_unique_id_;
                next_unique_id_++;
                txn_requests_.Push(txn);
                mutex_.Unlock();
            }
            else
            {
                // Apply all writes
                mutex_.Lock();
                ApplyWrites(txn);
                
                // Mark transaction as committed via committed_txns_
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;

                // Update relevant data structure
                txn_results_.Push(txn);
                mutex_.Unlock();
            }
        }
    }
}

void TxnProcessor::ExecuteTxnParallel(Txn* txn)
{
    // Get the current commited transaction index for the further validation.
    // Record start time
    txn->occ_start_idx_ = committed_txns_.Size();

    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();
    
    // <Start of critical section>
    active_set_mutex_.Lock();

    // Make a copy of the active set save it
    set<Txn*> active_set_copy = active_set_.GetSet();

    // Add this transaction to the active set
    active_set_.Insert(txn);

    // <End of critical section>
    active_set_mutex_.Unlock();
    
    int occ_end_idx = committed_txns_.Size();
    bool valid = true;
    int i;

    // Validation phase:

    // Check overlap with each committed record whose 
    // key appears in the txn's read and write sets
    for (i = txn->occ_start_idx_; i < occ_end_idx; i++)
    {
        Txn* commit_txn = committed_txns_[i];

        for (Key write_key : commit_txn->writeset_)
        {
            for (Key read_key : txn->readset_)
            {
                if (write_key == read_key)
                {
                    valid = false;
                }
            }
        }
    }

    // For each active txn, check if current txn read or write set overlaps
    for (Txn* active_txn: active_set_copy)
    {
        for (Key write_key : active_txn->writeset_)
        {
            for (Key read_key : txn->readset_)
            {
                if (write_key == read_key)
                {
                    valid = false;
                }
            }

            for (Key write_key2 : txn->writeset_)
            {
                if (write_key == write_key2)
                {
                    valid = false;
                }
            }
        }
    }

    if (!valid)
    {
        
        active_set_mutex_.Lock();
        // Remove this transaction from the active set
        active_set_.Erase(txn);
        active_set_mutex_.Unlock();

        // Clean-up transaction
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;

        // Restart transaction
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
    else
    {
        mutex_.Lock();
        // Apply all writes
        ApplyWrites(txn);
        mutex_.Unlock();

        active_set_mutex_.Lock();
        // Remove this transaction from the active set
        active_set_.Erase(txn);
        active_set_mutex_.Unlock();

        // Mark transaction as committed via committed_txns_
        mutex_.Lock();
        committed_txns_.Push(txn);
        txn->status_ = COMMITTED;

        // Update relevant data structure
        txn_results_.Push(txn);
        mutex_.Unlock();
    }
}

void TxnProcessor::RunOCCParallelScheduler()
{
    //
    // Implement this method! Note that implementing OCC with parallel
    // validation may need to create another method, like
    // TxnProcessor::ExecuteTxnParallel.
    // Note that you can use active_set_ and active_set_mutex_ we provided
    // for you in the txn_processor.h
    //
    // [For now, run serial scheduler in order to make it through the test
    // suite]
    Txn* txn;
    while (!stopped_)
    {
        // Get the next new transaction request (if one is pending) and pass it to an execution thread.
        if (txn_requests_.Pop(&txn))
        {
            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->ExecuteTxnParallel(txn); });
        }
    }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn)
{
    // Read all necessary data for this transaction from storage 
    // (Note that unlike the version of MVCC from class, you should lock the key before each read)
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        storage_->Lock(*it);
        if (storage_->Read(*it, &result, txn->unique_id_)) txn->reads_[*it] = result;
        storage_->Unlock(*it);
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        storage_->Lock(*it);
        if (storage_->Read(*it, &result, txn->unique_id_)) txn->reads_[*it] = result;
        storage_->Unlock(*it);
    }

    // Execute txn's program logic.
    txn->Run();

    // Acquire all locks for keys in the write_set_
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        storage_->Lock(*it);
    }

    bool valid = true;
    
    //Call MVCCStorage::CheckWrite method to check all keys in the write_set_
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        valid = valid || storage_->CheckWrite(*it, txn->unique_id_);
    }

    if (!valid)
    {
        // Release all locks for keys in the write_set_
        for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
        {
            storage_->Unlock(*it);
        }

        // Cleanup txn
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;

        // Completely restart the transaction.
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
    else
    {
        // Apply all writes
        ApplyWrites(txn);

        // Release all locks for keys in the write_set_
        for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
        {
            storage_->Unlock(*it);
        }

        // Mark transaction as committed via committed_txns_
        mutex_.Lock();
        committed_txns_.Push(txn);
        txn->status_ = COMMITTED;

        // Update relevant data structure
        txn_results_.Push(txn);
        mutex_.Unlock();
    }    
}

void TxnProcessor::RunMVCCScheduler()
{
    //
    // Implement this method!

    // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
    // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn.
    //
    // [For now, run serial scheduler in order to make it through the test
    // suite]

    Txn* txn;
    while (!stopped_)
    {
        // Get the next new transaction request (if one is pending) and pass it to an execution thread.
        if (txn_requests_.Pop(&txn))
        {
            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->MVCCExecuteTxn(txn); });
        }
    }
}

// This function shows how you can choose the correct partition to find a value given the key
StaticThreadPool TxnProcessor::GetPartitionThreadPool(Key key) 
{
    uint64 chunk_size = (uint64) (dbsize_ / PARTITION_THREAD_COUNT);
    uint64 index = key / chunk_size;

    StaticThreadPool found = partition_threads_.Get(index);

    return found;
}

void TxnProcessor::RunHStoreScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Get the next new transaction request (if one is pending) and pass it to an execution thread.
        if (txn_requests_.Pop(&txn))
        {
            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->HStoreExecuteTxn(txn); });
        }
    }
}


/* TODO: Implement Command Router / transaction coordinator.
   Coordinator spawns worker thread by calling HStoreExecuteTxn */
void TxnProcessor::HStoreExecuteTxn(Txn* txn)
{

/* 
For one-shot/single-site/sterile transactions, the worker thread sends the txn to the appropriate partition thread.
    <ThreadPoolFromArray>.AddTask([this, txn]() { this->HStorePartitionThreadExecuteTxn(txn); });
    Check the finished queue for results. When results arrive, worker thread will add results to the committed txn list
For multi-partition transactions, determine which threads own the requested data. 
    Decompose the transaction into subplans. Send each txn to the thread
        When creating subplans, populate the hstore_start_time_, hstore_subplan_mutex_, and h_store_subplan_cond_ fields
        To send next subplan and guarantee its run next, add a new method to StaticThreadPool that pushes "priority task" to front of queue
    Command Router receives commits/aborts from threads. It sends the final decision to each thread to formally commit or abort the transaction.
    Track rate of aborts via counter. If the number of aborts exceeds some threshold: then switch to the intermediate strategy via this::strategy_
        We will set an epoch ourselves.
    If aborts continue - then switch to advanced this::strategy_
    If all commit, check the finished queue for results. When results arrive, worker thread will add joined results to the committed txn list
*/

}

/* TODO: Implement logic that each partition thread performs. Assume each partition
   contains a thread pool with 1 thread inside it. Task submission and retrieval is abstracted away.
   We will use the partition parameter to restrict tasks to only read values inside their partition
   
   i.e. if partition = 1, then only read and write values from [ (db_size/ partition_count)*partition, (db_size/ partition_count)*(partition+1) )  */
void TxnProcessor::HStorePartitionThreadExecuteTxn(Txn* txn, int partition)
{
/*
If strategy_ = 0: Implement basic H-Store concurrency control
    For Put() and Expect():
        Commits the transaction and place results inside finished queue
            Only interact with keys in readset_ and writeset_ inside the provided partition
    For RMW():
        Hold the txn for X time. 
        If any txns come in during X time that have a lower timestamp than X, then abort. 
            Check timestamp by reading all values in queue after X time.
                Implement a new function inside static_thread_pool.h to accomplish this: IsMostRecentTxn(txn)
        Else, execute the next subplans sent in by the command router. we do not need to hold the subplan again (PENDING)
            Edit: we may need to hold the subplan again. Pending discussion
        Each thread then sends its decision back to the Command Router.
            Use txn->hstore_subplan_mutex_, and txn->h_store_subplan_cond_ to accomplish this
        Waits for a response back from the command router of whether to commit/abort. 
        If commit, then place results inside a finished queue
If strategy = 1: Implement intermediate H-Store concurrency control 
    Double X to increase wait time
If strategy = 2: Implement advanced H-store concurrency control
    When running subplan, if plan breaks OCC then abort.
        Start timestamp is the timestamp received upon entering the system.
    If commit, then place results inside a finished queue
*/
}