#pragma once

#include <unordered_map>
#include <string>

#include "utils.h"
#include "ska/bytell_hash_map.hpp"


namespace NTFRParser {

    struct TParseState {
        uint64_t Dimension;
        uint64_t CurrIndex;
        std::shared_ptr<std::string> StrPtr;
    };


    class ISingleRecordParser {
    public:
        virtual ~ISingleRecordParser() = default;

        virtual void ParseDictionaryRecord(
                char *ptr,
                std::shared_ptr<std::string> outSharedPtr,
                unsigned size,
                ska::bytell_hash_map<std::string, ska::bytell_hash_map<uint64_t, std::string> >& forwardIndexer,
                ska::bytell_hash_map<std::string, ska::bytell_hash_map<std::string, uint64_t> >& backwardIndexer,
                uint64_t order
        ) = 0;
    };

    class TFRRecordParser : public ISingleRecordParser {
    public:
        ~TFRRecordParser() override = default;
        void ParseDictionaryRecord(
                char *ptr,
                std::shared_ptr<std::string> outSharedPtr,
                unsigned size,
                ska::bytell_hash_map<std::string, ska::bytell_hash_map<uint64_t, std::string> >& forwardIndexer,
                ska::bytell_hash_map<std::string, ska::bytell_hash_map<std::string, uint64_t> >& backwardIndexer,
                uint64_t order
        ) override;
    };
}
