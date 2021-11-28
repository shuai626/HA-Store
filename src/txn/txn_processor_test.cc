#include "txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"

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

void Benchmark(const vector<LoadGen*>& lg)
{
    // Number of transaction requests that can be active at any given time.
    int active_txns = 100;
    deque<Txn*> doneTxns;

    // For each MODE...
    for (CCMode mode = SERIAL; mode <= MVCC; mode = static_cast<CCMode>(mode + 1))
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
                TxnProcessor* p = new TxnProcessor(mode);

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
    cout << "\t\t-------------------------------------------------------------------" << endl;
    cout << "\t\t                Average Transaction Duration" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    cout << "\t\t0.1ms\t\t1ms\t\t10ms\t\t(0.1ms, 1ms, 10ms)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;

    vector<LoadGen*> lg;

    cout << "\t\t            Low contention Read only (5 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
    lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
    lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));
    lg.push_back(new RMWDynLoadGen(1000000, 5, 0, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            Low contention Read only (30 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.0001));
    lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.001));
    lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.01));
    lg.push_back(new RMWDynLoadGen(1000000, 30, 0, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            High contention Read only (5 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
    lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
    lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));
    lg.push_back(new RMWDynLoadGen(100, 5, 0, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            High contention Read only (30 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(100, 30, 0, 0.0001));
    lg.push_back(new RMWLoadGen(100, 30, 0, 0.001));
    lg.push_back(new RMWLoadGen(100, 30, 0, 0.01));
    lg.push_back(new RMWDynLoadGen(100, 30, 0, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            Low contention read-write (5 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
    lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
    lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));
    lg.push_back(new RMWDynLoadGen(1000000, 0, 5, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            Low contention read-write (10 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
    lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
    lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));
    lg.push_back(new RMWDynLoadGen(1000000, 0, 10, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            High contention read-write (5 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
    lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
    lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));
    lg.push_back(new RMWDynLoadGen(100, 0, 5, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    cout << "\t\t            High contention read-write (10 records)" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen(100, 0, 10, 0.0001));
    lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
    lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));
    lg.push_back(new RMWDynLoadGen(100, 0, 10, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();

    // 80% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    cout << "\t\t            High contention mixed read only/read-write" << endl;
    cout << "\t\t-------------------------------------------------------------------" << endl;
    lg.push_back(new RMWLoadGen2(50, 30, 10, 0.0001));
    lg.push_back(new RMWLoadGen2(50, 30, 10, 0.001));
    lg.push_back(new RMWLoadGen2(50, 30, 10, 0.01));
    lg.push_back(new RMWDynLoadGen2(50, 30, 10, {0.0001, 0.001, 0.01}));

    Benchmark(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();
}
