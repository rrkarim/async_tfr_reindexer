#include <pybind11/stl_bind.h>

#include "utils.h"
#include "bind_tools.h"
#include "features_parser.h"
#include "pool_parser.h"
#include <memory>
#include <unordered_map>
#include <iostream>

using namespace std;

namespace NTFRParser {

    namespace {

        class TExceptionHandler {
            static std::unordered_map<int64_t, std::string> errorMessages;
        public:
            static void addMessage(int64_t key, const std::string &message) {
                errorMessages[key] = message;
            }

            static std::string getMessage(int64_t key) {
                return errorMessages.at(key);
            }
        };

        std::unordered_map<int64_t, std::string> TExceptionHandler::errorMessages(0);

        std::string GetErrorMessage(int64_t key) {
            return TExceptionHandler::getMessage(key);
        }

        void AddMessage(const unsigned &workerId, const std::string &message) {
            TExceptionHandler::addMessage(workerId, message);
        }

        inline void KillableWrite(const int fd, const char *buf, size_t size) {
            ssize_t ret;
            while (size > 0) {
                do {
                    ret = write(fd, buf, size);
                } while ((ret < 0) && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK));
                if (ret < 0) {
                    throw std::runtime_error(strerror(errno));
                }
                size -= ret;
                buf += ret;
            }
        }


        template<typename TFunctor>
        inline void RegisterFunctorRunner(py::module &m) {
            m.def("run_functor_in_cpp", [](
                    TThreadPoolHandle threadHandle,
                    // Functor comes from Python,
                    // we pass a shared_ptr to ensure that functor lives
                    // in both Python and C++, this is especially important in case of
                    // exceptions when both runtimes are shutting down almost independenly
                    // Also, we prefer extracting values from functor itself, not returning them
                    std::shared_ptr<TFunctor> functor,
                    int fd,
                    unsigned worker_id
            ) {
                threadHandle.ThreadPool->push(
                        [functor /*we are sure functor lives in thread*/, fd, worker_id](int) mutable {
                            try {
                                (*functor)();
                            } catch (const std::exception &ex) {
                                AddMessage(worker_id, ex.what());
                                worker_id |= 1u << 31u;  // this will indicate about error
                                KillableWrite(fd, (const char *) (&worker_id), 4);
                                throw;
                            } catch (...) {
                                AddMessage(worker_id, "NO AVAILABLE ex->what()");
                                worker_id |= 1u << 31u;  // this will indicate about error
                                KillableWrite(fd, (const char *) (&worker_id), 4);
                                throw;
                            }
                            KillableWrite(fd, (const char *) (&worker_id), 4);
                        }
                );
            });
        }

        inline void RegisterThreadBridgeStuff(py::module &m) {
            m.def("get_thread_bridge_error_message", &GetErrorMessage);
        }


        struct TFRPythonParser {
            std::shared_ptr<TFRParser> parser;

            TFRPythonParser(
                    TThreadPoolHandle dispatcherThreadPoolHandle,
                    const std::vector<std::string>& forwardFeatureFileNames,
                    const std::vector<std::string>& backwardFeatureFileNames,
                    uint64_t maxYieldSize = 3
            )
            : parser(std::make_shared<TFRParser>(
                    dispatcherThreadPoolHandle,
                    forwardFeatureFileNames,
                    backwardFeatureFileNames,
                    maxYieldSize
            )) {}
        };


        class TParserRecordWaiter {
            TFRPythonParser handle;
            double waitTime;
            int64_t recsCount;
        public:
            TParserRecordWaiter(TFRPythonParser x)
                    : handle(x)
                    , waitTime(0.0)
                    , recsCount(0)
            {}

            void operator()() {
                TimeCalcer calcerGuard(&waitTime);
                recsCount = handle.parser->WaitReady();
            }

            double getWaitTime() {
                return waitTime;
            }

            int64_t getRecordsCount() {
                return recsCount;
            }
        };

        class TParserJobAssigner {
            TFRPythonParser Handle;
            BatchedStringHandle Job;
            uint64_t AssignedCount;
            int _index;
        public:
            TParserJobAssigner(
                    TFRPythonParser x,
                    BatchedStringHandle job,
                    int index
            )
                    : Handle(x)
                    , Job(job)
                    , _index(index)
            {}

            void operator()() {
                AssignedCount = Handle.parser->AssignNewJob(Job, _index);
            }

            uint64_t getAssignedRecordsCount() {
                return AssignedCount;
            }
        };

    }
    void register_tools(py::module &m) {
        py::class_<BatchedStringHandle>(m, "BatchedStringHandle", py::module_local(), py::buffer_protocol())
                // in python code, allow to create only from bytes
                        // yes, it's possible to use c++ copy constructor in the __init__
                .def(py::init<std::string&>())
                .def(py::init<const BatchedStringHandle &>())
                .def_buffer([](BatchedStringHandle &self) -> py::buffer_info {
                    auto _internals = self.getStringInternals();
                    return py::buffer_info(
                            (void *) (_internals.first),
                            sizeof(char),
                            py::format_descriptor<char>::format(),
                            1,
                            {_internals.second},
                            {sizeof(char)}
                    );
                })
                .def("getString", [](const BatchedStringHandle &s) -> const py::bytes {
                    return py::bytes(s.getString());
                }, py::return_value_policy::reference_internal)
                .def("getOutString", [](const BatchedStringHandle &s) -> const py::bytes {
                    return py::bytes(s.getOutString());
                }, py::return_value_policy::reference_internal)
                .def("__len__", [](BatchedStringHandle &self) -> size_t {
                    return self.getStringInternals().second;
                })
                .def("__sizeof__", [](BatchedStringHandle &self) { return self.getStringInternals().second; });


        // Parser stuff
        py::class_<TFRPythonParser>(m, "TFRPythonParser", py::module_local())
                .def(py::init<
                        TThreadPoolHandle,
                        const vector<std::string>&,
                        const vector<std::string>&,
                        uint64_t
                     >())
                .def("start", [](TFRPythonParser &self) { self.parser->Start(); })
                .def("stop", [](TFRPythonParser &self) { self.parser->Stop(); })
                .def("next", [](TFRPythonParser &self) { return self.parser->Next(); });

        py::class_<TParserRecordWaiter, std::shared_ptr<TParserRecordWaiter>>
                (m, "TFRParserWaiter", py::module_local())
                .def(py::init<TFRPythonParser>())
                .def("getWaitTime", &TParserRecordWaiter::getWaitTime)
                .def("getRecordsCount", &TParserRecordWaiter::getRecordsCount);
        RegisterFunctorRunner<TParserRecordWaiter>(m);

        py::class_<TParserJobAssigner, std::shared_ptr<TParserJobAssigner>>
                (m, "TFRParserJobAssigner", py::module_local())
                .def(py::init<TFRPythonParser, BatchedStringHandle, int>())
                .def("getAssignedRecordsCount", &TParserJobAssigner::getAssignedRecordsCount);
        RegisterFunctorRunner<TParserJobAssigner>(m);

        // Thread pool stuff
        py::class_<TThreadPoolHandle>(m, "ThreadPoolHandle", py::module_local())
                .def(py::init<>())
                .def(py::init<unsigned>())
                .def("stop", &TThreadPoolHandle::Stop)
                .def("size", &TThreadPoolHandle::Size)
                .def("resize", &TThreadPoolHandle::Resize);

        RegisterThreadBridgeStuff(m);

    }
}