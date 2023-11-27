#pragma once
#include <memory>
#include <cstring>
#include <chrono>
#include <string>

#include <pybind11/numpy.h>
#include <pybind11/stl_bind.h>


#define STATIC_CHECK(x, message) if (!(x)) { throw std::runtime_error(message); }


namespace NTFRParser {

    struct TimeCalcer {
        double *value;
        decltype(std::chrono::steady_clock::now()) start;

        TimeCalcer(double *val);
        ~TimeCalcer();
    };

    class BatchedStringHandle {
        std::shared_ptr<std::string> _shP, _outShared;
    public:
        BatchedStringHandle();
        ~BatchedStringHandle() = default;

        BatchedStringHandle(const BatchedStringHandle &other) = default;
        BatchedStringHandle(std::string& cpp_string);
        const std::string& getString() const;
        const std::string& getOutString() const;
        std::shared_ptr<std::string> getOtputPtr();
        std::pair<const char *, size_t> getStringInternals() const;
    };
}