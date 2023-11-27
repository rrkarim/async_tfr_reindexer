add_library(protos)
target_include_directories(protos
        PUBLIC
        .
        ${CMAKE_CURRENT_BINARY_DIR}
        )

set(PROTO_SOURCES feature.proto example.proto)

protobuf_generate_cpp(LIB_SOURCES LIB_HEADERS ${PROTO_SOURCES} )

link_directories(${CMAKE_PREFIX_PATH}/lib)
target_link_libraries(protos ${Protobuf_LIBRARIES})

target_sources(protos
        PUBLIC
        ${LIB_HEADERS}
        PRIVATE
        ${LIB_SOURCES}
        )