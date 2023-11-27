cmake_minimum_required(VERSION 3.22)
project(tfr_torch_tensor_parser)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=1 -fPIC")

find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)
find_package(utf8_range REQUIRED)


find_package(Python3 COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})

include_directories(${PROTOBUF_INCLUDE_DIRS})

add_subdirectory(protos)


set(SOURCE_FILES
        src/features_parser.cpp
        src/features_parser.h
        src/pool_parser.cpp
        src/pool_parser.h
        src/typedefs.h
        src/utils.cpp
        src/utils.h
        )

add_library(tfr_torch_tensor_parser SHARED ${SOURCE_FILES})

set(protobuf_ABSL_USED_TARGETS
        absl::absl_check
        absl::absl_log
        absl::algorithm
        absl::base
        absl::bind_front
        absl::bits
        absl::btree
        absl::cleanup
        absl::cord
        absl::core_headers
        absl::debugging
        absl::die_if_null
        absl::dynamic_annotations
        absl::flags
        absl::flat_hash_map
        absl::flat_hash_set
        absl::function_ref
        absl::hash
        absl::layout
        absl::log_initialize
        absl::log_severity
        absl::memory
        absl::node_hash_map
        absl::node_hash_set
        absl::optional
        absl::span
        absl::status
        absl::statusor
        absl::strings
        absl::synchronization
        absl::time
        absl::type_traits
        absl::utility
        absl::variant
        )

target_link_libraries(
        tfr_torch_tensor_parser
        PUBLIC
        protos
        ${Python3_LIBRARIES}
        ${PROTOBUF_LIBRARY}
        ${protobuf_ABSL_USED_TARGETS}
        utf8_range::utf8_validity
)

target_include_directories(
        tfr_torch_tensor_parser
        PUBLIC
        protos
        src
        third_party
        ${PYTHON_INCLUDE_DIRS}
)

add_subdirectory(pybind)