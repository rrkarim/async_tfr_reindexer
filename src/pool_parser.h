#pragma once
#include <mutex>
#include <queue>
#include <vector>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "utils.h"
#include "features_parser.h"

// third party includes
#include "ctpl/ctpl_stl.h"
#include "ska/bytell_hash_map.hpp"


namespace NTFRParser {

    using ThreadPoolType = ctpl::thread_pool;

    struct TThreadPoolHandle {
        std::shared_ptr<ThreadPoolType> ThreadPool;

        TThreadPoolHandle(unsigned numThreads = 1);

        void Stop();

        size_t Size() const;

        void Resize(unsigned newSize);
    };

    class TJobsSynchronizer {
        std::mutex Lock;
        std::condition_variable CondVar;
        uint64_t JobsCount;
        std::exception_ptr ErrorExc;
        bool IsError;
    public:
        TJobsSynchronizer()
                : Lock()
                , CondVar()
                , JobsCount()
                , ErrorExc(nullptr)
                , IsError(false)
        {}

        ~TJobsSynchronizer() = default;

        inline auto MakeGuard() noexcept {
            return std::unique_lock<std::mutex> (Lock);
        }

        inline void WaitJobs(const std::string &defaultMessage = "Error in WaitJobs") {
            auto guard = MakeGuard();
            CondVar.wait(guard, [this]() -> bool { return JobsCount == 0 || IsError; });
            if (IsError) {
                if (ErrorExc) {
                    std::rethrow_exception(ErrorExc);
                } else {
                    throw std::runtime_error(defaultMessage);
                }
            }
        }

        inline void Clear() noexcept {
            IsError = false;
            JobsCount = 0;
            ErrorExc = nullptr;
        }

        // Pay attention that this method does NOT block!!!
        // User is required to make guard via MakeGuard and then insert all necessary jobs
        template <typename TFunction>
        inline void RegisterJob(ThreadPoolType &threadPool, TFunction &&func) noexcept {
            threadPool.push([func = std::forward<TFunction>(func), this](int threadPoolId) mutable noexcept {
                try {
                    func(threadPoolId);
                } catch (...) {
                    IsError = true;
                    this->ErrorExc = std::current_exception();
                    CondVar.notify_one();
                }
                auto guard = MakeGuard();
                JobsCount--;
                if(!JobsCount) {
                    CondVar.notify_one();
                }
            });
            JobsCount++;
        }
    };

    class TSafeDieCall {
        std::mutex DieLock;
        std::atomic<bool> isDeadFlag;
        std::exception_ptr ErrorExc;
        std::function<void()> DieCallback;
    public:
        TSafeDieCall(std::function<void()> &&dieCallback = &TSafeDieCall::DefaultCallback)
                : DieLock()
                , isDeadFlag(false)
                , ErrorExc(nullptr)
                , DieCallback(dieCallback)
        {}

        inline bool IsDead() noexcept {
            return isDeadFlag;
        }

        /// Try-catch is performed without blocking for performance reasons in multithread code
        /// This means that if first thread throws, then context-switch when we perform catch(...),
        /// other thread throws - we will obtain exception from second thread
        /// This may be a real race condition if we Call() without outer locks,
        /// especially if second thread failed because first thread has failed
        /// In reality, we could not observe this even in tests
        template <typename TFunction>
        auto operator()(TFunction &&fn) {
            ThrowIfDead();
            try {
                return fn();
            } catch (...) {
                std::unique_lock<std::mutex> guard(DieLock);
                isDeadFlag = true;
                // save the first exception to throw - other may be causations of the first
                // and then non-informative
                if (ErrorExc == nullptr) {
                    ErrorExc = std::current_exception();
                    // Called only once on firstmost exception
                    try {
                        DieCallback();
                    } catch (...) {
                        // If user's die callback has failed, he should fix die callback first
                        // and then fix the error which has caused failure
                        // that's why we set more priority to DieCallback() exception message
                        ErrorExc = std::current_exception();
                        throw;
                    }
                }
                throw;
            }
        }

        inline void ThrowIfDead() {
            if (isDeadFlag) {
                std::unique_lock<std::mutex> guard(DieLock);
                std::rethrow_exception(ErrorExc);
            }
        }

    private:
        static inline void DefaultCallback() noexcept {}
    };

    using TForwardFeatureIndexer = ska::bytell_hash_map<std::string, ska::bytell_hash_map<uint64_t, std::string> >;
    using TBackwardFeatureIndexer = ska::bytell_hash_map<std::string, ska::bytell_hash_map<std::string, uint64_t> >;

    class TFRParser {
        struct TReferenceKeep {
            // Sometimes we benchmark parser on parsing same records
            // In this case we schedule same Handle multiple times
            // We don't have unique key in this case so we just use this Counter
            uint64_t Counter;
            BatchedStringHandle Handle;
        };

        struct TStreamState {
            uint64_t RowIdCounter;
            uint64_t QueueSize;
            mutable std::condition_variable AssignConditionVar;

            TStreamState();
            ~TStreamState() = default;
        };

        struct TPJob {
            uint64_t Id;
            std::string_view String;
            int Index;
            std::shared_ptr<std::string> OutSharedPtr;
        };

        struct TPJobComparator {
            inline bool operator()(const TPJob& lhs, const TPJob& rhs) {
                // less Id => higher priority
                return lhs.Id > rhs.Id;
            }
        };

        mutable std::mutex Lock;

        std::priority_queue<
            TPJob,
            std::vector<TPJob>,
            TPJobComparator
        > QueueToParse;
        TStreamState StreamState;

        ska::bytell_hash_map<const char *, TReferenceKeep> ToParseStringReferenceKeeper;
        // TODO: no need to template this for now
        TForwardFeatureIndexer ForwardFeatureIndexer;
        TBackwardFeatureIndexer BackwardFeatureIndexer;

        mutable std::condition_variable ParseCondVar;

        // yield attributes
        std::queue<int64_t> YieldQueue;
        mutable std::condition_variable YieldCondVar;
        uint64_t MaxYieldSize;

        // cpu-intensive resources

        uint64_t ParserThreadsNum, ParsersCurrentNum;
        std::shared_ptr<ctpl::thread_pool> MainThreadPool, DispatchThreadPool;
        TJobsSynchronizer DispatcherSynchronizer;
        TSafeDieCall SafeDieCaller;

        std::atomic<bool> StopFlag;
    public:
        TFRParser(
                TThreadPoolHandle dispatcherThreadPoolHandle,
                const std::vector<std::string>& forwardFeatureFileNames,
                const std::vector<std::string>& backwardFeatureFileNames,
                uint64_t maxYieldSize = 1
        );
        ~TFRParser();

        void Start();
        void Stop();

        int64_t WaitReady();
        int64_t Next();

        uint64_t AssignNewJob(
                BatchedStringHandle handle,
                int index
        );

    private:
        void WaitImpl(std::unique_lock<std::mutex> &guard, std::condition_variable &condVar, std::function<bool()> &&callback);
        void WaitCanAssignNewParseJob();
        void DispatcherThread();
        // std::vector<TDictionaryLikeRecord> GetSplittedRecord(TDictionaryLikeRecord &&currentRecord);

        // thread unsafe
        bool ParsingIsDone();
    };
}
