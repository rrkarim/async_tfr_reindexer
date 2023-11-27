#include "utils.h"
#include <type_traits>
#include <iostream>
using namespace std;

namespace NTFRParser {

    BatchedStringHandle::BatchedStringHandle(std::string& cpp_string) {
        // copy the object to heap
        _shP = make_shared<std::string>(cpp_string);
        _outShared = make_shared<std::string>();
    }

    std::pair<const char *, size_t> BatchedStringHandle::getStringInternals() const {
        return {_shP->data(), _shP->size()};
    }

    std::shared_ptr<std::string> BatchedStringHandle::getOtputPtr() {
        return _outShared;
    }

    const std::string& BatchedStringHandle::getString() const {
        return *_shP;
    }

    const std::string& BatchedStringHandle::getOutString() const {
        return *_outShared;
    }

    TimeCalcer::TimeCalcer(double *val)
            : value(val), start(std::chrono::steady_clock::now()) {}

    TimeCalcer::~TimeCalcer() {
        *value = double((std::chrono::steady_clock::now() - start).count()) / 1e9;
    }
}