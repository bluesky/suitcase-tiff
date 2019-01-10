from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


# Suitcase subpackages must follow strict naming and interface conventions. The
# public API should include some subset of the following. Any functions not
# implemented should be omitted, rather than included and made to raise
# NotImplementError, so that a client importing this library can immediately
# know which portions of the suitcase API it supports without calling any
# functions.
#
# def export(...)
#     ...
#
#
# def ingest(...):
#     ...
#
#
# def reflect(...):
#     ...
#
#
# handlers = []
