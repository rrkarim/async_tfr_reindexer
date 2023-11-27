#include "pool_parser.h"
#include <iostream>
#include <fstream>
using namespace std;

namespace NTFRParser {

    TThreadPoolHandle::TThreadPoolHandle(unsigned numThreads)
            : ThreadPool(new ThreadPoolType(numThreads)) {}

    void TThreadPoolHandle::Stop() {
        ThreadPool->stop(false);
    }

    void TThreadPoolHandle::Resize(unsigned newSize) {
        ThreadPool->resize(newSize);
    }

    size_t TThreadPoolHandle::Size() const {
        return ThreadPool->size();
    }

    TFRParser::TStreamState::TStreamState()
            : RowIdCounter(0)
            , QueueSize()
            , AssignConditionVar()
    {}

    namespace {
        void initializeFeatureMapperFromFile(
                TForwardFeatureIndexer& forwardIndexer,
                TBackwardFeatureIndexer& backwardIndexer,
                const std::vector<std::string>& forwardFeatureFileNames,
                const std::vector<std::string>& backwardFeatureFileNames) {

            // TODO: parallelize this
            const auto getNumLines = [](const std::string& fileName) -> uint64_t {
                std::ifstream file(fileName);
                std::string line;
                uint64_t lineCount = 0;
                while (getline(file, line)) {
                    ++lineCount;
                }
                file.close();
                return lineCount;
            };

            for(const auto& featureFileName : forwardFeatureFileNames) {
                forwardIndexer.emplace(
                        featureFileName.substr(0, featureFileName.length() - 4),
                        TForwardFeatureIndexer::mapped_type());

                forwardIndexer[featureFileName.substr(0, featureFileName.length() - 4)].reserve(getNumLines(featureFileName));

                std::ifstream file(featureFileName);
                std::ios_base::sync_with_stdio(false);
                std::string line;
                uint64_t lineIndex = 1;
                while (getline(file, line)) {
                    forwardIndexer[featureFileName.substr(0, featureFileName.length() - 4)].emplace(lineIndex++, line);
                }
                file.close();
            }
            for(const auto& featureFileName : backwardFeatureFileNames) {
                backwardIndexer.emplace(
                        featureFileName.substr(0, featureFileName.length() - 4),
                        TBackwardFeatureIndexer::mapped_type());
                backwardIndexer[featureFileName.substr(0, featureFileName.length() - 4)].reserve(getNumLines(featureFileName));
                std::ifstream file(featureFileName);
                std::ios_base::sync_with_stdio(false);
                std::string line;
                uint64_t lineIndex = 1;
                while (getline(file, line)) {
                    backwardIndexer[featureFileName.substr(0, featureFileName.length() - 4)].emplace(line, lineIndex++);
                }
                file.close();
            }
        }
    }


    TFRParser::TFRParser(
            TThreadPoolHandle dispatcherThreadPoolHandle,
            const std::vector<std::string>& forwardFeatureFileNames,
            const std::vector<std::string>& backwardFeatureFileNames,
            uint64_t maxYieldSize
    )
        : Lock()
        , QueueToParse()
        , StreamState()
        , ParseCondVar()
        , YieldQueue()
        , YieldCondVar()
        , MaxYieldSize(maxYieldSize)
        , ParserThreadsNum(dispatcherThreadPoolHandle.ThreadPool->size())
        , ParsersCurrentNum(0)
        , StopFlag(false)
        , SafeDieCaller([this] { Stop(); })
        , DispatchThreadPool(dispatcherThreadPoolHandle.ThreadPool)
        , DispatcherSynchronizer()
    {
        auto &streamState = StreamState;
        streamState.QueueSize = 0;
        initializeFeatureMapperFromFile(ForwardFeatureIndexer, BackwardFeatureIndexer,
                                        forwardFeatureFileNames, backwardFeatureFileNames);
    }

    uint64_t TFRParser::AssignNewJob(
            BatchedStringHandle handle,
            int index
    ) {
        WaitCanAssignNewParseJob();
        const auto &[stringPtr, stringLen] = handle.getStringInternals();
        auto outSharedPtr = handle.getOtputPtr();
        // too lazy to change the code here
        auto currentJobs = std::vector<std::string_view>({std::string_view(stringPtr, stringLen)});
        {
            std::unique_lock<std::mutex> guard(Lock);
            uint64_t &streamRowIdCounter = StreamState.RowIdCounter;
            for (auto&& currentJob : currentJobs) {
                auto &&[iterator, isInserted] = ToParseStringReferenceKeeper.emplace(
                    currentJob.data(),
                    TReferenceKeep{
                        .Counter = 1,
                        .Handle = handle,
                    }
                );
                if(!isInserted) {
                    iterator->second.Counter += 1;
                }
                QueueToParse.emplace(TPJob{
                    .Id = streamRowIdCounter++,
                    .String = currentJob,
                    .Index = index,
                    .OutSharedPtr = outSharedPtr,
                });
            }
            StreamState.QueueSize += currentJobs.size();
        }
        ParseCondVar.notify_all();
        return currentJobs.size();
    }

    void TFRParser::WaitCanAssignNewParseJob() {
        std::unique_lock<std::mutex> guard(Lock);
        WaitImpl(guard, StreamState.AssignConditionVar, [&]() {
            return StreamState.QueueSize < MaxYieldSize;
        });
    }

    int64_t TFRParser::WaitReady() {
        std::unique_lock<std::mutex> guard(Lock);
        WaitImpl(guard, YieldCondVar, [&]() {
           return !YieldQueue.empty() || ParsingIsDone();
        });
        if(!YieldQueue.empty()) {
            return YieldQueue.size();
        } else {
            return -1;
        }
    }

    int64_t TFRParser::Next() {
        int64_t outIndex;
        {
            std::unique_lock<std::mutex> guard(Lock);
            if(YieldQueue.empty()) {
                throw std::runtime_error("Yield queue is empty. First need to wait for the queue.");
            }
            outIndex = YieldQueue.front();
            YieldQueue.pop();

            StreamState.QueueSize--;
            StreamState.AssignConditionVar.notify_all();
        }
        return outIndex;
    }

    void TFRParser::WaitImpl(std::unique_lock<std::mutex> &guard, std::condition_variable &condVar, std::function<bool()> &&callback) {
        condVar.wait(guard, [this, callback = std::forward<std::function<bool()>>(callback)] {
            return callback() || SafeDieCaller.IsDead();
        });
        SafeDieCaller.ThrowIfDead();
    }

    void TFRParser::Start() {
        auto guard = DispatcherSynchronizer.MakeGuard();
        DispatcherSynchronizer.Clear();
        ParsersCurrentNum = ParserThreadsNum;
        for(uint64_t i = 0; i < ParserThreadsNum; ++i) {
            DispatcherSynchronizer.RegisterJob(*DispatchThreadPool, [this](int) {
                this->SafeDieCaller([&]() { this->DispatcherThread(); });
            });
        }
     }

    void TFRParser::Stop() {
        StopFlag = true;
        ParseCondVar.notify_all();
        YieldCondVar.notify_all();
        StreamState.AssignConditionVar.notify_all();
    }

    TFRParser::~TFRParser() {
        Stop();
        int64_t readyValue;
        while((readyValue = WaitReady() != -1)) {
            for(int i = 0; i < readyValue; ++i) {
                Next();
            }
        }
        DispatcherSynchronizer.WaitJobs("Waiting C++ parser jobs in destructor");
    }

    bool TFRParser::ParsingIsDone() {
        return StopFlag && QueueToParse.empty() && ParsersCurrentNum == 0;
    }

    namespace {
        struct _TWorkingParsersGuard {
            uint64_t &Value;
            std::mutex &Lock;
            std::condition_variable &CondVar;

            _TWorkingParsersGuard(uint64_t &value, std::mutex &lock, std::condition_variable &condVar)
                    : Value(value), Lock(lock), CondVar(condVar)
            {}

            ~_TWorkingParsersGuard() {
                std::unique_lock<std::mutex> guard(Lock);
                Value--;
                CondVar.notify_all();
            }
        };
    }

    void TFRParser::DispatcherThread() {
        _TWorkingParsersGuard currentlyWorkingGuard(ParsersCurrentNum, Lock, YieldCondVar);

        while (true) {
            std::string_view newJob;
            int index;
            std::shared_ptr<std::string> outSharedPtr;
            {
                std::unique_lock<std::mutex> guard(Lock);
                WaitImpl(guard, ParseCondVar, [&]() {
                    return !QueueToParse.empty() || StopFlag;
                });
                if (StopFlag && QueueToParse.empty()) {
                    return;
                }

                const auto &curParseRef = QueueToParse.top();
                newJob = curParseRef.String;
                index = curParseRef.Index;
                outSharedPtr = curParseRef.OutSharedPtr;
                QueueToParse.pop();
            }
            auto parser = TFRRecordParser();
            parser.ParseDictionaryRecord(const_cast<char*>(newJob.data()),
                                            outSharedPtr,
                                            newJob.size(),
                                            ForwardFeatureIndexer,
                                            BackwardFeatureIndexer,
                                            index);

            {
                std::unique_lock<std::mutex> Guard(Lock);
                YieldQueue.emplace(index);
                if (--ToParseStringReferenceKeeper.at(newJob.data()).Counter == 0) {
                    ToParseStringReferenceKeeper.erase(newJob.data());
                }
            }

            YieldCondVar.notify_all();
        }
    }
}