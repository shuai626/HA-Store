#include "txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

// Handler to print out stack trace on crashes (for DEBUG ONLY)
void handler(int sig) {
  void *array[10];
  size_t size;

  size = backtrace(array, 10);

  // Print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode)
{
    switch (mode)
    {
        case SERIAL:
            return " Serial   ";
        case LOCKING_EXCLUSIVE_ONLY:
            return " Locking A";
        case LOCKING:
            return " Locking B";
        case OCC:
            return " OCC      ";
        case P_OCC:
            return " OCC-P    ";
        case MVCC:
            return " MVCC     ";
        case H_STORE:
            return " H-Store     ";
        default:
            return "INVALID MODE";
    }
}

class LoadGen
{
   public:
    virtual ~LoadGen() {}
    virtual Txn* NewTxn() = 0; 
};

class RMWLoadGen : public LoadGen
{
   public:
    RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), wait_time_(wait_time)
    {
    }

    virtual Txn* NewTxn() { return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_); }
   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    double wait_time_;
};

class RMWLoadGen2 : public LoadGen
{
   public:
    RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), wait_time_(wait_time)
    {
    }

    virtual Txn* NewTxn()
    {
        // 80% of transactions are READ only transactions and run for the full
        // transaction duration. The rest are very fast (< 0.1ms), high-contention
        // updates.
        if (rand() % 100 < 80)
            return new RMW(dbsize_, rsetsize_, 0, wait_time_);
        else
            return new RMW(dbsize_, 0, wsetsize_, 0);
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    double wait_time_;
};

class RMWDynLoadGen : public LoadGen
{
   public:
    
    RMWDynLoadGen(int dbsize, int rsetsize, int wsetsize, vector<double> wait_times)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize)
    {
        wait_times_ = wait_times;
    }

    virtual Txn* NewTxn()
    {
        // Mix transactions with different time durations (wait_times_) 
        if (rand() % 100 < 30)
            return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[0]);
        else if (rand() % 100 < 60)
            return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[1]);
        else
            return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[2]);
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    vector<double> wait_times_;
};

class RMWDynLoadGen2 : public LoadGen
{
   public:
    RMWDynLoadGen2(int dbsize, int rsetsize, int wsetsize, vector<double> wait_times)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize)
    {
        wait_times_ = wait_times;
    }

    virtual Txn* NewTxn()
    {
        // 80% of transactions are READ only transactions and run for the different
        // transaction duration. The rest are very fast (< 0.1ms), high-contention
        // updates.
        if (rand() % 100 < 80) {
            // Mix transactions with different time durations (wait_times_) 
            if (rand() % 100 < 30)
                return new RMW(dbsize_, rsetsize_, 0, wait_times_[0]);
            else if (rand() % 100 < 60)
                return new RMW(dbsize_, rsetsize_, 0, wait_times_[1]);
            else
                return new RMW(dbsize_, rsetsize_, 0, wait_times_[2]);
        } else {
            return new RMW(dbsize_, 0, wsetsize_, 0);
        }
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    vector<double> wait_times_;
};

/*
    This test performs Single Site Read and Single Site Write transactions. Dynamic timing.
*/
class SingleSiteLoadGen : public LoadGen
{
   public:
    
    SingleSiteLoadGen(int dbsize, int setsize, double wait_time, int read_only_pct, int write_only_pct, 
        int thread_count)
        : dbsize_(dbsize), setsize_(setsize), ss_read_only_ratio_(read_only_pct), ss_write_only_ratio_(write_only_pct), thread_count_(thread_count)
    {
        wait_time_ = wait_time;
    }

    virtual Txn* NewTxn()
    {
        //used to determine read_only, write_only, or multipartition
        int nxt = rand() % 100;
        int txn_readset_size = setsize_;
        int txn_writeset_size = setsize_;

        if (nxt < ss_read_only_ratio_) 
        {
            txn_writeset_size = 0;
        } 
        else
        {
            txn_readset_size = 0;
        } 

        return new RMW(dbsize_, txn_readset_size, txn_writeset_size, 1, thread_count_, wait_time_);
    }

   private:
    int dbsize_;
    int setsize_;
    int ss_read_only_ratio_;
    int ss_write_only_ratio_;
    int thread_count_;
    double wait_time_;
};

class SingleSiteDynLoadGen : public LoadGen
{
   public:
    
    SingleSiteDynLoadGen(int dbsize, int setsize, vector<double> wait_times, int read_only_pct, int write_only_pct, 
        int thread_count)
        : dbsize_(dbsize), setsize_(setsize), ss_read_only_ratio_(read_only_pct), ss_write_only_ratio_(write_only_pct), thread_count_(thread_count)
    {
        wait_times_ = wait_times;
    }

    virtual Txn* NewTxn()
    {
        //used to determine read_only, write_only, or multipartition
        int nxt = rand() % 100;
        int txn_readset_size = setsize_;
        int txn_writeset_size = setsize_;

        if (nxt < ss_read_only_ratio_) 
        {
            txn_writeset_size = 0;
        } 
        else
        {
            txn_readset_size = 0;
        } 

        if (rand() % 100 < 30)
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size, 1, thread_count_, wait_times_[0]);
        else if (rand() % 100 < 60)
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size,  1, thread_count_, wait_times_[1]);
        else
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size,  1, thread_count_, wait_times_[2]);

    }

   private:
    int dbsize_;
    int setsize_;
    int ss_read_only_ratio_;
    int ss_write_only_ratio_;
    int thread_count_;
    vector<double> wait_times_;
};

/*
    Performs SingleSiteReadOnly/SingleSiteWriteOnly/Multipartition transactions with Dynamic timing. 

    read_only_pct: percent of single site read only transactions
    write_only_pct: percent of single site write only transactions
    multipartition: percent of multipartition transacations
    max_partitions: the number of partitions for each multipartition transaction
*/
class MultipartitionAndSingleSiteLoadGen : public LoadGen
{
   public:
    
    MultipartitionAndSingleSiteLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time, int read_only_pct, int write_only_pct, 
        int multipartition_pct, int max_partitions, int thread_count)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), ss_read_only_ratio_(read_only_pct), ss_write_only_ratio_(write_only_pct), 
        multipartition_ratio_(multipartition_pct), max_partitions_(max_partitions), thread_count_(thread_count)
    {
        wait_time_ = wait_time;
    }

    virtual Txn* NewTxn()
    {
        //used to determine read_only, write_only, or multipartition
        int nxt = rand() % 100;
        int txn_readset_size = rsetsize_;
        int txn_writeset_size = wsetsize_;
        int txn_k = max_partitions_;

        if (nxt < ss_read_only_ratio_) 
        {
            txn_k = 1;
            txn_writeset_size = 0;
            // makes sure all three txn's are the same size
            txn_readset_size += wsetsize_;
        } 
        else if (nxt < (ss_read_only_ratio_ + ss_write_only_ratio_))
        {
            txn_k = 1;
            txn_readset_size = 0;
            // makes sure all three txn's are the same size
            txn_writeset_size += rsetsize_;
        } 

        return new RMW(dbsize_, txn_readset_size, txn_writeset_size, txn_k, thread_count_, wait_time_);
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    int ss_read_only_ratio_;
    int ss_write_only_ratio_;
    int multipartition_ratio_;
    int max_partitions_;
    int thread_count_;
    double wait_time_;
};

/*
    Performs SingleSiteReadOnly/SingleSiteWriteOnly/Multipartition transactions with Dynamic timing. 
    
    read_only_pct: percent of single site read only transactions
    write_only_pct: percent of single site write only transactions
    multipartition: percent of multipartition transacations
    max_partitions: the number of partitions for each multipartition transaction
*/
class MultipartitionAndSingleSiteDynLoadGen : public LoadGen
{
   public:
    
    MultipartitionAndSingleSiteDynLoadGen(int dbsize, int rsetsize, int wsetsize, vector<double> wait_times, int read_only_pct, int write_only_pct, 
        int multipartition_pct, int max_partitions, int thread_count)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), ss_read_only_ratio_(read_only_pct), ss_write_only_ratio_(write_only_pct), 
        multipartition_ratio_(multipartition_pct), max_partitions_(max_partitions), thread_count_(thread_count)
    {
        wait_times_ = wait_times;
    }

    virtual Txn* NewTxn()
    {
        //used to determine read_only, write_only, or multipartition
        int nxt = rand() % 100;
        int txn_readset_size = rsetsize_;
        int txn_writeset_size = wsetsize_;
        int txn_k = max_partitions_;

        if (nxt < ss_read_only_ratio_) 
        {
            txn_k = 1;
            txn_writeset_size = 0;
            // makes sure all three txn's are the same size
            txn_readset_size += wsetsize_;
        } 
        else if (nxt < (ss_read_only_ratio_ + ss_write_only_ratio_))
        {
            txn_k = 1;
            txn_readset_size = 0;
            // makes sure all three txn's are the same size
            txn_writeset_size += rsetsize_;
        } 

        //RMW(int dbsize, int readsetsize, int writesetsize, int k, int thread_count,  double time = 0)
        // Mix transactions with different time durations (wait_times_) 
        if (rand() % 100 < 30)
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size, txn_k, thread_count_, wait_times_[0]);
        else if (rand() % 100 < 60)
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size,  txn_k, thread_count_, wait_times_[1]);
        else
            return new RMW(dbsize_, txn_readset_size, txn_writeset_size,  txn_k, thread_count_, wait_times_[2]);
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    int ss_read_only_ratio_;
    int ss_write_only_ratio_;
    int multipartition_ratio_;
    int max_partitions_;
    int thread_count_;
    vector<double> wait_times_;
};


//Generates benchmark with a given database size
void Benchmark(const vector<LoadGen*>& lg, int dbsize, double wait_time = 0.0000001, int config = 0, int PARTITION_THREAD_COUNT = 8)
{
    // Number of transaction requests that can be active at any given time.
    int active_txns = 100;
    deque<Txn*> doneTxns;

    // For each MODE...
    for (CCMode mode = SERIAL; mode <= H_STORE; mode = static_cast<CCMode>(mode + 1))
    {
        // Print out mode name.
        cout << ModeToString(mode) << flush;

        // For each experiment, run 2 times and get the average.
        for (uint32 exp = 0; exp < lg.size(); exp++)
        {
            double throughput[2];
            for (uint32 round = 0; round < 2; round++)
            {
                int txn_count = 0;

                // Create TxnProcessor in next mode.
                TxnProcessor* p = new TxnProcessor(mode, dbsize, wait_time, config, PARTITION_THREAD_COUNT);

                // Record start time.
                double start = GetTime();

                // Start specified number of txns running.
                for (int i = 0; i < active_txns; i++) p->NewTxnRequest(lg[exp]->NewTxn());

                // Keep 100 active txns at all times for the first full second.
                while (GetTime() < start + 0.5)
                {
                    Txn* txn = p->GetTxnResult();
                    doneTxns.push_back(txn);
                    txn_count++;
                    p->NewTxnRequest(lg[exp]->NewTxn());
                }

                // Wait for all of them to finish.
                for (int i = 0; i < active_txns; i++)
                {
                    Txn* txn = p->GetTxnResult();
                    doneTxns.push_back(txn);
                    txn_count++;
                }

                // Record end time.
                double end = GetTime();

                throughput[round] = txn_count / (end - start);
                for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it)
                {
                    delete *it;
                }

                doneTxns.clear();
                delete p;
            }

            // Print throughput
            cout << "\t" << (throughput[0] + throughput[1]) / 2 << "\t" << flush;
        }

        cout << endl;
    }
}

int main(int argc, char** argv)
{
    signal(SIGSEGV, handler);
    signal(SIGABRT, handler);

    int PARTITION_THREAD_COUNT = 8;
    int db_size = 100;
    
    cout << "\t\t-------------------------------------------------------------------" << endl;
    cout << "\t\t                Average Transaction Duration" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    cout << "\t\t0.1ms\t\t1ms\t\t10ms\t\t(0.1ms, 1ms, 10ms)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;

    vector<LoadGen*> lg;

    for (int j = 0; j < 3; j++)
    {
         //Ratio 33/33/33 ss_read/ss_write/multipartition. 6 records so it's divided evenly betwwen read/write;
        cout << "\t\t           HSTORE" << j << "Varying MPT size (6 records): 100% MPT, 8 partition threads, 1 microsecond wait" << endl;
        cout << "\t\t-------------------------------------------------------------------" << endl;
        
        int arr[5] = {1,2,4,6,8};

        for (int i = 0; i < 5; i++)
        {   
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.0001, 0, 0, 100, arr[i], PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.001, 0, 0, 100,  arr[i],  PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.01, 0, 0, 100,  arr[i],  PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteDynLoadGen(db_size, 3, 3, {0.0001, 0.001, 0.01}, 0, 0, 100,  arr[i],  PARTITION_THREAD_COUNT));
            Benchmark(lg, db_size, 0.0000001, j, PARTITION_THREAD_COUNT);
            cout << endl;
            for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
                lg.clear();
        }
    }
    
    for (int j = 0; j < 3; j++)
    {
        cout << "\t\t           HSTORE" << j << " Varying Ratio size (6 records): 4 MPT, 8 partition threads, 1 microsecond wait" << endl;
        cout << "\t\t-------------------------------------------------------------------" << endl;
    
        int arr1[11] = {100,90,80,70,60,50,40,30,20,10,0};

        for (int i = 0; i < 11; i++)
        {   
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.0001, 0, arr1[i], 100, 4, PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.001, 0, arr1[i], 100,  4,  PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.01, 0, arr1[i], 100,  4,  PARTITION_THREAD_COUNT));
            lg.push_back(new MultipartitionAndSingleSiteDynLoadGen(db_size, 3, 3, {0.0001, 0.001, 0.01}, 0, arr1[i], 100,  4,  PARTITION_THREAD_COUNT));
        
            Benchmark(lg, db_size, 0.000001, j, PARTITION_THREAD_COUNT);
            cout << endl;

            for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
                lg.clear();
        }
    }

    for (int j = 0; j < 3; j++)
    {
        int arr1[2] = {100,90};
        for (int k = 0; k < 2; k++)
        {
            cout << "\t\t           HSTORE" << j << "Varying PARTITION_THREAD_COUNT (6 records): 4 MPT, 1 microsecond wait " << arr1[k] << endl;
            cout << "\t\t-------------------------------------------------------------------" << endl;

            int arr2[6] = {1,2,4,8,16};

            for  (int i = 0; i < 5; i++)
            {   
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.0001, 0, arr1[k], 100, 4, arr2[i]));
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.001, 0, arr1[k], 100,  4,  arr2[i]));
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.01, 0, arr1[k], 100,  4,  arr2[i]));
                lg.push_back(new MultipartitionAndSingleSiteDynLoadGen(db_size, 3, 3, {0.0001, 0.001, 0.01}, 0, arr1[k], 100,  4,  arr2[i]));

                Benchmark(lg, db_size, 0.0000001, j, arr2[i]);
                cout << endl;

                for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
                    lg.clear();
            }
        }
    }   

    for (int j = 0; j < 3; j++)
    {
        int arr1[2] = {100,90};
        for (int k = 0; k < 2; k++)
        {
            cout << "\t\t           HSTORE" << j << "Varying WAIT_TIME (6 records): 4 MPT, 8 PARTITION_THREADS, RATIO: " << arr1[k] << endl;
            cout << "\t\t-------------------------------------------------------------------" << endl;

            double arr2[5] = {0.000001, 0.00001, 0.0001,0.001,0.01};

            for  (int i = 0; i < 5; i++)
            {   
                std::cout << "WAIT TIME: " << arr2[i] << std::endl;
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.0001, 0, arr1[k], 100, 4, 8));
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.001, 0, arr1[k],  100, 4, 8));
                lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.01, 0, arr1[k],  100, 4, 8));
                lg.push_back(new MultipartitionAndSingleSiteDynLoadGen(db_size, 3, 3, {0.0001, 0.001, 0.01}, 0, arr1[k], 100, 4, 8));

                Benchmark(lg, db_size, arr2[i], j, PARTITION_THREAD_COUNT);
                cout << endl;

                for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
                    lg.clear();
            }
        }
    }   

    int ratio[2] = {100,90};
    for (int k = 0; k < 2; k++)
    {
        cout << "\t\t           ALL CONCURRENCY CONTROLS:  1 microsecond, 2 MPT, 8 PARTITION_THREADS, RATIO: " << ratio[k] << endl;
        cout << "\t\t-------------------------------------------------------------------" << endl;

        lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.0001, 0, ratio[k], 100, 2, 8));
        lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.001, 0, ratio[k],  100, 2, 8));
        lg.push_back(new MultipartitionAndSingleSiteLoadGen(db_size, 3, 3, 0.01, 0, ratio[k],  100, 2, 8));
        lg.push_back(new MultipartitionAndSingleSiteDynLoadGen(db_size, 3, 3, {0.0001, 0.001, 0.01}, 0, ratio[k], 100, 2, 8));

        Benchmark(lg, db_size, 0.000001, 0, PARTITION_THREAD_COUNT);
        cout << endl;

        for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
            lg.clear();
    }
}
