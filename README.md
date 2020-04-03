# suitcase.tiff

This is a suitcase subpackage for reading a particular file format.

## Installation

```
pip install suitcase-tiff
```

## Quick Start

`suitcase-tiff` supports stack and series.

Using stack:

```
import suitcase.tiff_stack
docs = db[-1].documents(fill=True)
suitcase.tiff_stack.export(docs, 'my_exported_files/', file_prefix='PREFIX-')
```

Using series:

```
import suitcase.tiff_series
docs = db[-1].documents(fill=True)
suitcase.tiff_series.export(docs, 'my_exported_files/', file_prefix='PREFIX-')
```

## Documentation

See the [suitcase documentation](https://nsls-ii.github.io/suitcase).
