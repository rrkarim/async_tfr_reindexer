find_package(pybind11 REQUIRED)

pybind11_add_module(libpoolparser module.cpp bind_tools.cpp bind_tools.h)
target_link_libraries(libpoolparser PRIVATE tfr_torch_tensor_parser pybind11::module)