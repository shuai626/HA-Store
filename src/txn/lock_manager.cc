#include "lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }

bool LockManagerA::WriteLock(Txn* txn, const Key& key)
{
    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);

        // If deque already contains txn, return false and No-OP
        for (LockRequest lr : *d)
        {
            if (lr.txn_ == txn)
            {
                return false;
            }
        }
    }
    catch (const std::exception & e)
    {
        d = new deque<LockRequest>();
        lock_table_.emplace(key, d);
    }

    // Add transaction to Deque using EXCLUSIVE LockRequest
    LockRequest* x = new LockRequest(EXCLUSIVE, txn);

    // If Deque was empty before adding LockRequest, return true 
    if (d->size() == 0) 
    {
        d->emplace_back(*x);
        return true;
    }
    else
    {
        d->emplace_back(*x);
        try
        {
            // If Transaction has value in txn_waits_, increments by 1
            txn_waits_.at(txn) += 1;
        }
        catch (const std::exception & e)
        {
            // Else, add new value into txn_waits_ with value 1
            txn_waits_.emplace(txn, 1);
        }
        return false;
    }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key)
{
    // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
    // simply use the same logic as 'WriteLock'.
    return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key)
{
    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);
    }
    catch (const std::exception & e)
    {
        return;
    }
    
    // Remove LockRequest with current Txn from Deque
    bool removed = false;
    int i;
    for (i = 0; i < d->size(); i++)
    {
        if (d->at(i).txn_ == txn)
        {
            d->erase(d->begin()+i);
            removed = true;
            break;
        }
    }

    if (!removed || d->size() == 0)
    {
        return;
    }

    // If LockRequest was at head of deque
    if (i == 0)
    {
        Txn* new_txn = d->front().txn_;
        
        // Decrement value of txn_waits_ by 1 for new head
        try 
        {
            txn_waits_.at(new_txn) -= 1;
        }
        catch (const std::exception & e)
        {
            return;
        }
        

        // If txn_waits_[txn] == 0, then add txn to ready_txns_ queue
        if (txn_waits_.at(new_txn) == 0)
        {
            ready_txns_->emplace_back(new_txn);
        }
    }    
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners)
{
    // Delete values in owners (since not assumed to be empty)
    owners->clear();

    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);
    }
    catch (const std::exception & e)
    {
        return UNLOCKED;
    }

    if (d->size() == 0)
    {
        return UNLOCKED;
    }
    else
    {
        Txn* head_txn = d->front().txn_;
        owners->push_back(head_txn);
        return EXCLUSIVE;
    }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }

bool LockManagerB::WriteLock(Txn* txn, const Key& key)
{
    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);

        // If deque already contains txn, return false and No-OP
        for (LockRequest lr : *d)
        {
            if (lr.txn_ == txn)
            {
                return false;
            }
        }
    }
    catch (const std::exception & e)
    {
        d = new deque<LockRequest>();
        lock_table_.emplace(key, d);
    }

    // Add transaction to Deque using EXCLUSIVE LockRequest
    LockRequest* x = new LockRequest(EXCLUSIVE, txn);

    // If Deque was empty before adding LockRequest, return true 
    if (d->size() == 0) 
    {
        d->emplace_back(*x);
        return true;
    }
    else
    {
        d->emplace_back(*x);
        try
        {
            // If Transaction has value in txn_waits_, increments by 1
            txn_waits_.at(txn) += 1;
        }
        catch (const std::exception & e)
        {
            // Else, add new value into txn_waits_ with value 1
            txn_waits_.emplace(txn, 1);
        }
        return false;
    }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key)
{
    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);

        // If deque already contains txn, return false and No-OP
        for (LockRequest lr : *d)
        {
            if (lr.txn_ == txn)
            {
                return false;
            }
        }
    }
    catch (const std::exception & e)
    {
        d = new deque<LockRequest>();
        lock_table_.emplace(key, d);
    }

    // Add transaction to Deque using SHARED LockRequest
    LockRequest* x = new LockRequest(SHARED, txn);

    // If all LockRequests in deque are SHARED lock, return true
    bool can_grant = true;
    for (LockRequest lr : *d)
    {
        if (lr.mode_ != SHARED)
        {
            can_grant = false;
        }
    }

    if (can_grant) 
    {
        d->emplace_back(*x);
        return true;
        
    }
    else
    {
        d->emplace_back(*x);
        try
        {
            // If Transaction has value in txn_waits_, increments by 1
            txn_waits_.at(txn) += 1;
        }
        catch (const std::exception & e)
        {
            // Else, add new value into txn_waits_ with value 1
            txn_waits_.emplace(txn, 1);
        }
        return false;
    }
}

void LockManagerB::Release(Txn* txn, const Key& key)
{
   // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try 
    {
        d = lock_table_.at(key);
    }
    catch (const std::exception & e)
    {
        return;
    }
    
    // Remove LockRequest with current Txn from Deque
    bool removed = false;
    int i;
    LockMode prev_lock_mode;
    for (i = 0; i < d->size(); i++)
    {
        if (d->at(i).txn_ == txn)
        {
            prev_lock_mode = d->at(i).mode_;
            d->erase(d->begin()+i);
            removed = true;
            break;
        }
    }

    if (!removed || d->size() == 0)
    {
        return;
    }

    // Shared lock grants transaction IFF it is at the front and next lock is exclusive
    LockMode new_lock_mode = d->front().mode_;
    if (i == 0 && prev_lock_mode == SHARED && new_lock_mode == EXCLUSIVE)
    {
        Txn* new_txn = d->front().txn_;
        // Decrement value of txn_waits_ by 1 for new head
        try 
        {
            txn_waits_.at(new_txn) -= 1;
        }
        catch (const std::exception & e)
        {
            return;
        }

        // If txn_waits_[txn] == 0, then add txn to ready_txns_ queue
        if (txn_waits_.at(new_txn) == 0)
        {
            txn_waits_.erase(new_txn);
            ready_txns_->emplace_back(new_txn);
        }
    }
    // Exclusive lock grants transaction if it is at front
    else if (i == 0 && prev_lock_mode == EXCLUSIVE)
    {
        if (new_lock_mode == EXCLUSIVE)
        {
            Txn* new_txn = d->front().txn_;
            // Decrement value of txn_waits_ by 1 for new head
            try 
            {
                txn_waits_.at(new_txn) -= 1;
            }
            catch (const std::exception & e)
            {
                return;
            }

            // If txn_waits_[txn] == 0, then add txn to ready_txns_ queue
            if (txn_waits_.at(new_txn) == 0)
            {
                txn_waits_.erase(new_txn);
                ready_txns_->emplace_back(new_txn);
            }
        }
        else if (new_lock_mode == SHARED)
        {
            int j = 0;
            while (j < d->size() && d->at(j).mode_ == SHARED)
            {
                try 
                {
                    Txn* new_txn = d->at(j).txn_;
                    txn_waits_.at(new_txn) -= 1;

                    if (txn_waits_.at(new_txn) == 0)
                    {
                        txn_waits_.erase(new_txn);
                        ready_txns_->emplace_back(new_txn);
                    }
                }
                catch (const std::exception & e)
                {
                }

                j++;
            }
        }
    }
    // Exclusive lock grants transaction if SHARED lock prefix is larger
    else if (prev_lock_mode == EXCLUSIVE && new_lock_mode == SHARED) 
    {
        int j = 0;
        while (j < d->size() && d->at(j).mode_ == SHARED)
        {
            try 
            {
                Txn* new_txn = d->at(j).txn_;
                txn_waits_.at(new_txn) -= 1;

                if (txn_waits_.at(new_txn) == 0)
                {
                    txn_waits_.erase(new_txn);
                    ready_txns_->emplace_back(new_txn);
                }
            }
            catch (const std::exception & e)
            {
            }

            j++;
        }
    }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners)
{
    // Delete values in owners (since not assumed to be empty)
    owners->clear();

    // Grab deque using key from lock_table_
    deque<LockRequest>* d;
    try
    {
        d = lock_table_.at(key);
    }
    catch (const std::exception & e)
    {
        return UNLOCKED;
    }
    
    // If deque is empty
    if (d->size() == 0)
    {
        return UNLOCKED;
    }
    // Else if current LockRequest is EXCLUSIVE
    else if (d->front().mode_ == EXCLUSIVE)
    {
        Txn* head_txn = d->front().txn_;
        owners->push_back(head_txn);
        return EXCLUSIVE;
    }
    // Else current LockRequest is SHARED
    else 
    {
        int i = 0;
        while (i < d->size() && d->at(i).mode_ == SHARED)
        {
            owners->push_back(d->at(i).txn_);
            i++;
        }

        return SHARED;
    }
}