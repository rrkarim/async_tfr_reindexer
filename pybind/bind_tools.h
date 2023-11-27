#pragma once
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

namespace NTFRParser {
    namespace py = pybind11;
    void register_tools(py::module &m);
}