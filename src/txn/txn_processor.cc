#include "txn_processor.h"
#include <stdio.h>
#include <set>
#include <unordered_set>
#include <time.h>
#include <iostream>

#include "lock_manager.h"
#include "txn_types.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

// Wait time for different strategies in seconds
#define BASIC_WAIT_TIME        0.00001
#define INTERMEDIATE_WAIT_TIME 0.00002
#define ADVANCED_WAIT_TIME     0.00001

// Advanved H store on/off
#define ADVANCED_H_STORE_ON    0

// Abort thresholds
#define INTERMEDIATE_PLAN_THRESHOLD 0.05
#define ADVANCED_PLAN_THRESHOLD     0.1

TxnProcessor::TxnProcessor(CCMode mode, int dbsize, int partition_thread_count) : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1)
{
    if (mode_ == LOCKING_EXCLUSIVE_ONLY)
        lm_ = new LockManagerA(&ready_txns_);
    else if (mode_ == LOCKING)
        lm_ = new LockManagerB(&ready_txns_);
    
    // If mode_ == H_STORE, then "partition the database" and spawns StaticThreadPool of 1 thread for each partition.
    if (mode_ == H_STORE)
    {
        strategy_ = 0;
        abort_count_ = 0;
        dbsize_ = dbsize;
        partition_thread_count_ = partition_thread_count;

        for (int i = 0; i < partition_thread_count; i++) 
        {   
            StaticThreadPool* tp = new StaticThreadPool(1, i);
            partition_threads_.push_back(tp);
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

    if (mode_ == H_STORE) {
        for (StaticThreadPool* tp : partition_threads_)
        {
            delete tp;
        }
    }

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

// Return the partition thread that owns a given key
StaticThreadPool* TxnProcessor::GetPartitionThreadPool(Key key) 
{
    double chunk_size = ((double) dbsize_) /( (double) partition_thread_count_); 
    double calc = ((double)key) / chunk_size;
    uint64 index = (int ) calc;

    return partition_threads_[index];
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

void TxnProcessor::HStoreExecuteTxn(Txn* txn)
{
    // Initialize txn->occ_start_idx_ and txn->h_store_timestamp_
    txn->occ_start_idx_ = committed_txns_.Size();

    // Initialize mutex and cond for inter-thread communication
    txn->h_store_subplan_cond_ = PTHREAD_COND_INITIALIZER;
    txn->hstore_subplan_mutex_ = PTHREAD_MUTEX_INITIALIZER;

    txn->hstore_is_aborted_ = false;
    txn->hstore_is_first_phase_multitxn_ = true;

    pthread_mutex_lock(&txn->hstore_subplan_mutex_);

    // Find all necessary partition threads via GetPartitionThreadPool
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        txn->hstore_pending_partition_threads_.insert(GetPartitionThreadPool(*it));
    }
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        txn->hstore_pending_partition_threads_.insert(GetPartitionThreadPool(*it));
    }

    // Acquire locks for each partition thread
    for (std::set<StaticThreadPool*>::iterator it = txn->hstore_pending_partition_threads_.begin(); it != txn->hstore_pending_partition_threads_.end(); ++it)
    {
        StaticThreadPool* tp = *it;
        tp->mutex_.Lock();
    }

    // Coordinate transactions with partition threads
    for (std::set<StaticThreadPool*>::iterator it = txn->hstore_pending_partition_threads_.begin(); it != txn->hstore_pending_partition_threads_.end(); ++it)
    {
        StaticThreadPool* tp = *it;
        tp->AddTask([this, txn, tp]() {this->HStorePartitionThreadExecuteTxn(txn, tp); }, txn->hstore_start_time_, txn);
    }

    // Release locks for each partition thread
    for (std::set<StaticThreadPool*>::iterator it = txn->hstore_pending_partition_threads_.begin(); it != txn->hstore_pending_partition_threads_.end(); ++it)
    {
        StaticThreadPool* tp = *it;
        tp->mutex_.Unlock();
    }

    pthread_mutex_unlock(&txn->hstore_subplan_mutex_);

    // Wait for responses from all partition threads
    pthread_mutex_lock(&txn->hstore_subplan_mutex_);
    while (txn->hstore_pending_partition_threads_.size() > 0)
    {
        pthread_cond_wait(&txn->h_store_subplan_cond_, &txn->hstore_subplan_mutex_);
    }
    pthread_mutex_unlock(&txn->hstore_subplan_mutex_);  

    if (txn->hstore_is_aborted_)
    {
        return this->HStoreAbort(txn);
    }
    else if (txn->hstore_is_multipartition_transaction_)
    {
        pthread_mutex_lock(&txn->hstore_subplan_mutex_);
        // Prepare the next subplan for the partition threads
        // Find all necessary partition threads via GetPartitionThreadPool
        for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
        {
            txn->hstore_pending_partition_threads_.insert(GetPartitionThreadPool(*it));
        }
        txn->hstore_is_first_phase_multitxn_ = false;

        // Push task to queue
        for (std::set<StaticThreadPool*>::iterator it = txn->hstore_pending_partition_threads_.begin(); it != txn->hstore_pending_partition_threads_.end(); ++it)
        {
            StaticThreadPool* tp = *it;
            tp->AddTask([this, txn, tp]() {this->HStorePartitionThreadExecuteTxn(txn, tp); }, txn->hstore_start_time_, txn);
        }
        pthread_mutex_unlock(&txn->hstore_subplan_mutex_);

        // Wait for responses from all partition threads
        pthread_mutex_lock(&txn->hstore_subplan_mutex_);
        while (txn->hstore_pending_partition_threads_.size() > 0)
        {
            pthread_cond_wait(&txn->h_store_subplan_cond_, &txn->hstore_subplan_mutex_);
        }
        pthread_mutex_unlock(&txn->hstore_subplan_mutex_);

        if (txn->hstore_is_aborted_)
        {
            return this->HStoreAbort(txn);
        }    
    } 

    // Transaction commits, so add to finished queue and commited txn list
    txn->status_ = COMMITTED;

    mutex_.Lock();
    committed_txns_.Push(txn);
    ApplyWrites(txn);
    // Update relevant data structure
    txn_results_.Push(txn);
    mutex_.Unlock();
}

// Implement logic that each partition thread performs. Assume each partition
// contains a thread pool with 1 thread inside it. Task submission and retrieval is abstracted away.

// We will use the partition parameter to restrict tasks to only read values inside their partition
// i.e. if partition = 1, then only read and write values from [ (db_size/ partition_count)*partition, (db_size/ partition_count)*(partition+1) )  */
void TxnProcessor::HStorePartitionThreadExecuteTxn(Txn* txn, StaticThreadPool* tp)
{
    double wait_time = 0;

    //Hold the txn for X time depending on strategy 
    if (strategy_ == 0) 
    {
        wait_time = BASIC_WAIT_TIME;
    }
    else if (strategy_ == 1) 
    {
        wait_time = INTERMEDIATE_WAIT_TIME;
    }
    else 
    {
        wait_time = ADVANCED_WAIT_TIME;
    }
    
    hold(wait_time);

    vector<void*> recent_txns;
    
    // Check if any earlier, uncommitted Txns exist in the queue.
    tp->GetMostRecentTxnTimestamp(txn->hstore_start_time_, &recent_txns);

    // Perform current strategy's evaluation
    if(ADVANCED_H_STORE_ON || strategy_ == 2)
    {
        // For Advanced strategy, use OCC validation  
        bool valid = true;

        int i;
        
        // Check overlap with each record whose key appears in the txn's read and write sets
        for (i = 0; i < recent_txns.size(); i++)
        {
            Txn* prev_txn = (Txn *)recent_txns[i];

            for (Key write_key : prev_txn->writeset_)
            {
                if (txn->readset_.count(write_key) > 0)
                {
                    valid = false;
                }
            }
        }
        if(!valid)
        {
            txn->hstore_is_aborted_ =  true;
            HStoreRemovePartitionThread(txn,tp);
            return;
        }            
    }
    else 
    {
        // If there exists a lower, uncommited Txn
        if(recent_txns.size() != 0){
            txn->hstore_is_aborted_ =  true;
            HStoreRemovePartitionThread(txn,tp);
            return;
        }
    }

    if (!(txn->hstore_is_multipartition_transaction_))
    {
        HStoreExecuteReads(txn, tp);
        HStoreRun(txn, tp);
    }
    else if (txn->hstore_is_first_phase_multitxn_)
    {
        HStoreExecuteReads(txn, tp);
    }
    else
    {
        HStoreRun(txn, tp);
    }

    HStoreRemovePartitionThread( txn,tp);
}


/* HSTORE HELPERS */

void TxnProcessor::HStoreAbort(Txn* txn)
{
    pthread_mutex_unlock(&txn->hstore_subplan_mutex_);

    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Increase abort count and change strategy, if needed
    mutex_.Lock();
    this->abort_count_++;
    double abort_ratio = ((double) this->abort_count_) / (double (this->abort_count_ + committed_txns_.Size()));
    
    // Completely restart the transaction.
    // Cleanup txn
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();

    if (abort_ratio > ADVANCED_PLAN_THRESHOLD)
    {
        this->strategy_ = 2;
    }
    else if (abort_ratio > INTERMEDIATE_PLAN_THRESHOLD)
    {   
        this->strategy_ = 1;
    }
    else
    {
        this->strategy_ = 0;
    }
}

// Simulate waiting (in seconds) by running a for-loop*/
void TxnProcessor::hold(double time){
    double begin = GetTime();
    while (GetTime() - begin < time)
    {
    }
}

// Notify worker thread that current partition thread has completed their site's execution
void TxnProcessor::HStoreRemovePartitionThread(Txn* txn, StaticThreadPool* tp) {

    pthread_mutex_lock(&txn->hstore_subplan_mutex_);
    txn->hstore_pending_partition_threads_.erase(tp);
    pthread_cond_broadcast(&txn->h_store_subplan_cond_);
    pthread_mutex_unlock(&txn->hstore_subplan_mutex_);

}

void TxnProcessor::HStoreExecuteReads(Txn* txn, StaticThreadPool* tp){
    int partition = tp->GetIndex();

    double chunk_size = ((double) dbsize_) /( (double) partition_thread_count_);

    // Read everything in from readset in the partition.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        double calc = ((double) *it) / chunk_size;
        uint64 index = (int) calc;
        // Save each read result iff record exists in storage and is in the paritition.
        Value result;
        if(index == partition){
            if (storage_->Read(*it, &result)) 
            {
                mutex_.Lock();
                txn->reads_[*it] = result;
                mutex_.Unlock();
            }
        }
        
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        double calc = ((double) *it) / chunk_size;
        uint64 index = (int) calc;
        // Save each read result iff record exists in storage and is in the partition.
        Value result = 0;
        if(index == partition){
            if (storage_->Read(*it, &result))
            {
                mutex_.Lock();
                txn->reads_[*it] = result;
                mutex_.Unlock();
            } 
        }
    }
}


void TxnProcessor::HStoreRun(Txn* txn, StaticThreadPool* tp){
        int partition = tp->GetIndex();
        Value result = 0;

        double chunk_size = ((double) dbsize_) /( (double) partition_thread_count_);

        // Read everything in readset.
        for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
        {
            double calc = ((double) *it) / chunk_size;
            uint64 index = (int) calc;

            if(index == partition)
            {
                txn->Read(*it, &result);
            }
        } 

        // Run while loop to simulate the txn logic(duration is time_).
        hold(((RMW*)txn)->ReturnTxnTime());

        // Increment length of everything in writeset.
        for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
        {
            double calc = ((double) *it) / chunk_size;
            uint64 index = (int) calc;

            result = 0;
            if(index == partition){
                txn->Read(*it, &result);
                mutex_.Lock();
                txn->Write(*it, result + 1);
                mutex_.Unlock();
            }
        }

        do                         
        {                          
            txn->status_ = COMPLETED_C; 
            return;                
        } while (0);
}