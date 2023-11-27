#include "features_parser.h"
#include <string>
#include <iomanip>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <strstream>
#include <queue>

// include protos
#include "example.pb.h"

using namespace std;


namespace NTFRParser {

    namespace {
        constexpr uint32_t kCrc32CPolynomial = 0x82f63b78;
        std::array<uint32_t, 256> GenerateCrcTable() {
            std::array<uint32_t, 256> crcTable;
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t crc = i;
                for (uint32_t j = 0; j < 8; ++j) {
                    crc = (crc >> 1) ^ (kCrc32CPolynomial & static_cast<uint32_t>(-(crc & 1)));
                }
                crcTable[i] = crc;
            }
            return crcTable;
        }

        const std::array<uint32_t, 256> kCrcTable = GenerateCrcTable();

        uint32_t Crc32C(const uint8_t* data, size_t length) {
            uint32_t crc = 0xffffffff;
            for (size_t i = 0; i < length; ++i) {
                uint8_t index = (crc ^ data[i]) & 0xff;
                crc = (crc >> 8) ^ kCrcTable[index];
            }
            return crc ^ 0xffffffff;
        }

        uint32_t MaskedCrc32C(const uint8_t* data, size_t length) {
            uint32_t crc = Crc32C(data, length);
            // Rotate right by 15 bits and add a constant
            return ((crc >> 15) | (crc << 17)) + 0xa282ead8ul;
        }
    }

    template<typename FList, int B>
    void copyDataHelper(
            FList& featureList,
            const std::string& featureName,
            ska::bytell_hash_map<uint64_t, std::string>& forwardIndexer,
            ska::bytell_hash_map<std::string, uint64_t>& backwardIndexer,
            tensorflow::Example& targetExample
            ) {
        auto oldValue = featureList.value();

        tensorflow::Features *targetFeatures = targetExample.mutable_features();
        tensorflow::Feature new_feature;
        tensorflow::Int64List* int64_list = new_feature.mutable_int64_list();

        std::queue<std::string> stringQueue;

        std::string newValue;
        for(int i = 0; i < featureList.value_size(); ++i) {
            if (forwardIndexer.find(oldValue.Get(i)) == forwardIndexer.end()) {
                // empty is oov
                newValue = "";
            } else {
                newValue = forwardIndexer.at(oldValue.Get(i));
            }
            stringQueue.push(newValue);
        }
        uint64_t newIntValue;
        while(!stringQueue.empty()) {
            auto elem = stringQueue.front();
            if (backwardIndexer.find(elem) == backwardIndexer.end()) {
                newIntValue = 0;
            } else {
                newIntValue = backwardIndexer.at(elem);
            }
            int64_list->add_value(newIntValue);
            stringQueue.pop();
        }
        (*targetFeatures->mutable_feature())[featureName] = new_feature;
    }

    template<char DType>
    void CopyFeatureData(
            tensorflow::Feature& featureValue,
            tensorflow::Example& targetExample,
            const std::string& featureName,
            ska::bytell_hash_map<uint64_t, std::string>& forwardIndexer,
            ska::bytell_hash_map<std::string, uint64_t>& backwardIndexer
            ) {
        if constexpr (DType == 'i') {
            auto featureList = featureValue.int64_list();
            copyDataHelper<decltype(featureList), 8>(featureList, featureName, forwardIndexer, backwardIndexer, targetExample);
        } else if constexpr (DType == 'f') {
            // we support float64 only
            auto featureList = featureValue.float_list();
            copyDataHelper<decltype(featureList), 8>(featureList, featureName, forwardIndexer, backwardIndexer, targetExample);
        } else if constexpr (DType == 's') {
            auto featureList = featureValue.bytes_list();
            copyDataHelper<decltype(featureList), 8>(featureList, featureName, forwardIndexer, backwardIndexer, targetExample);
        }
    }

    void ParseExample(
            tensorflow::Example& example,
            tensorflow::Example& targetExample,
            ska::bytell_hash_map<std::string, ska::bytell_hash_map<uint64_t, std::string> >& forwardIndexer,
            ska::bytell_hash_map<std::string, ska::bytell_hash_map<std::string, uint64_t> >& backwardIndexer
    ) {
        auto featureMap = *example.mutable_features()->mutable_feature();
        tensorflow::Features* targetFeatures = targetExample.mutable_features();

        for (auto& [featureName, featureValue] : featureMap) {
            if(
                featureName != "creator_id_xf" &&
                featureName != "static_duration_xf" &&
                (featureName.find("_xf") != std::string::npos || featureName.find("_post_ids") != std::string::npos)
            ) {
                if(featureName.find("_xf") != std::string::npos) {
                    std::string indexerKey = featureName.substr(0, featureName.length() - 3);
                    CopyFeatureData<'i'>(featureValue, targetExample, featureName, forwardIndexer[indexerKey], backwardIndexer[indexerKey]);
                }
                else {
                    CopyFeatureData<'i'>(featureValue, targetExample, featureName, forwardIndexer["post_id"], backwardIndexer["post_id"]);
                }
            }
            else {
                tensorflow::Feature new_feature;
                new_feature.CopyFrom(featureValue);
                (*targetFeatures->mutable_feature())[featureName] = new_feature;
            }
        }
    }

    void TFRRecordParser::ParseDictionaryRecord(
            char *ptr,
            std::shared_ptr<std::string> outSharedPtr,
            unsigned size,
            ska::bytell_hash_map<std::string, ska::bytell_hash_map<uint64_t, std::string> >& forwardIndexer,
            ska::bytell_hash_map<std::string, ska::bytell_hash_map<std::string, uint64_t> >& backwardIndexer,
            uint64_t order
            ) {

        std::shared_ptr<std::string> dataBuffer = std::make_shared<std::string>();
        uint64_t length = 0;
        uint64_t index = 0;
        std::vector<std::pair<uint64_t, uint64_t>> indices;
        uint64_t ind = 0;
        std::string outputBuffer;
        uint64_t reservationLength = 12;

        while(index < size) {
            std::memcpy(&length, ptr + index, sizeof(length));
            // write currentLength

            index += sizeof(length);
            auto strView = std::string_view(ptr + index + 4, length);
            index += length + 8;
            tensorflow::Example example, targetExample;

            if (!example.ParseFromArray(reinterpret_cast<void *>(const_cast<char *>(strView.data())), length)) {
                throw std::runtime_error("Failed parsing given tfrecord file.");
            }

            ParseExample(example, targetExample, forwardIndexer, backwardIndexer);

            size_t sizeExample = example.ByteSizeLong();
            size_t targetSizeExample = targetExample.ByteSizeLong();


            std::string currOutputStr;
            if (!targetExample.SerializeToString(&currOutputStr)) {
                throw std::runtime_error("Failed serializing given tfrecord file.");
            }

            // FIXME: too much allocation happens here
            // need cached allocation implementation
            if (!ind) {
                reservationLength += 700000 * currOutputStr.size();
                outSharedPtr->reserve(reservationLength);
            }
            else if(ind * currOutputStr.size() > reservationLength) {
                reservationLength += 50000 * currOutputStr.size();
                outSharedPtr->reserve(reservationLength);
            }

            // crc32c
            uint32_t length_masked_crc = MaskedCrc32C(reinterpret_cast<const uint8_t *>(&targetSizeExample),
                                                      sizeof(targetSizeExample));
            uint32_t data_masked_crc = MaskedCrc32C(reinterpret_cast<const uint8_t *>(currOutputStr.data()),
                                                    targetSizeExample);
            outSharedPtr->append(reinterpret_cast<const char *>(&targetSizeExample), sizeof(targetSizeExample));
            outSharedPtr->append(reinterpret_cast<const char *>(&length_masked_crc), sizeof(length_masked_crc));
            *outSharedPtr += currOutputStr;
            outSharedPtr->append(reinterpret_cast<const char *>(&data_masked_crc), sizeof(data_masked_crc));
            ind += 1;
            if (ind % 5000 == 0) cout << "order: " << order << "ind: " << ind << endl;
        }
    }

}