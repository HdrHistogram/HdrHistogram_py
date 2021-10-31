
/* see https://python.readthedocs.io/en/stable/c-api/arg.html#strings-and-buffers */
#define PY_SSIZE_T_CLEAN
#include "Python.h"

static PyObject *py_hdr_encode(PyObject *self, PyObject *args);

static PyObject *py_hdr_decode(PyObject *self, PyObject *args);

static PyObject *py_hdr_add_array(PyObject *self, PyObject *args);
