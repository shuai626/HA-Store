#include "mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage()
{
    for (int i = 0; i < 1000000; i++)
    {
        Write(i, 0, 0);
        Mutex* key_mutex = new Mutex();
        mutexs_[i]       = key_mutex;
    }
}

// Free memory.
MVCCStorage::~MVCCStorage()
{
    for (auto it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it)
    {
        delete it->second;
    }

    mvcc_data_.clear();

    for (auto it = mutexs_.begin(); it != mutexs_.end(); ++it)
    {
        delete it->second;
    }

    mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list
void MVCCStorage::Lock(Key key)
{
    mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key)
{
    mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value *result, int txn_unique_id)
{
    //
    // Implement this method!
    deque<Version*>* version_lists;
    Version* version;

    try 
    {
        version_lists = mvcc_data_.at(key);

        // Iterate the version_lists and return the version whose write timestamp
        // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
        for (deque<Version*>::iterator it = version_lists->begin(); it != version_lists->end(); ++it)
        {
            version = *it;

            if (version->version_id_ <= txn_unique_id)
            {
                break;
            }
        }

        *result = version->value_;
        version->max_read_id_ = std::max(version->max_read_id_, txn_unique_id);
        return true;
    }
    catch (const std::exception & e)
    {
        return false;
    }
}

// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    deque<Version*>* version_lists;

    try 
    {
        version_lists = mvcc_data_.at(key);    

        // Before all writes are applied, we need to make sure that each write 
        // can be safely applied based on MVCC timestamp ordering protocol
        Version* version = version_lists->front();

        // Return true if this key passes the check, return false if not.
        // Update version list only if txn_unique_id is greater than or equal to latest version_id_
        return txn_unique_id > version->version_id_;
    }
    catch (const std::exception & e)
    {
        return false;
    }

}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id)
{
    //
    // Implement this method!

    //  Insert a new version (malloc a Version and specify its value/version_id/max_read_id) into the version_lists
    
    Version* version = (Version*)malloc(sizeof(Version));
    
    version->value_ = value;
    version->max_read_id_ = 0;
    version->version_id_ = txn_unique_id;

    deque<Version*>* version_lists;
    try 
    {
        version_lists = mvcc_data_.at(key);

        version_lists->emplace_front(version);
    }
    catch (const std::exception & e)
    {
        // No-OP
    }
}
