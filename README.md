# dissect.esedb

A Dissect module implementing a parser for Microsofts Extensible Storage Engine Database (ESEDB), used for example in
Active Directory, Exchange and Windows Update. For more information, please see [the
documentation](https://docs.dissect.tools/en/latest/projects/dissect.esedb/index.html).

## Requirements

This project is part of the Dissect framework and requires Python.

Information on the supported Python versions can be found in the Getting Started section of [the documentation](https://docs.dissect.tools/en/latest/index.html#getting-started).

## Installation

`dissect.esedb` is available on [PyPI](https://pypi.org/project/dissect.esedb/).

```bash
pip install dissect.esedb
```

This module is also automatically installed if you install the `dissect` package.

## Tools

### Impacket compatibility shim for secretsdump.py

Impacket does not ([yet](https://github.com/fortra/impacket/pull/1452)) have native support for `dissect.esedb`,
so in the meantime a compatibility shim is provided. To use this shim, simply install `dissect.esedb` using the 
instructions above, and execute `secretsdump.py` like so:

```bash
python -m dissect.esedb.tools.impacket /path/to/impacket/examples/secretsdump.py -h
```

Impacket `secretsdump.py` will now use `dissect.esedb` for parsing the `NTDS.dit` file, resulting in a nice performance improvement!

## Build and test instructions

This project uses `tox` to build source and wheel distributions. Run the following command from the root folder to build
these:

```bash
tox -e build
```

The build artifacts can be found in the `dist/` directory.

`tox` is also used to run linting and unit tests in a self-contained environment. To run both linting and unit tests
using the default installed Python version, run:

```bash
tox
```

For a more elaborate explanation on how to build and test the project, please see [the
documentation](https://docs.dissect.tools/en/latest/contributing/tooling.html).

## Contributing

The Dissect project encourages any contribution to the codebase. To make your contribution fit into the project, please
refer to [the development guide](https://docs.dissect.tools/en/latest/contributing/developing.html).

## Copyright and license

Dissect is released as open source by Fox-IT (<https://www.fox-it.com>) part of NCC Group Plc
(<https://www.nccgroup.com>).

Developed by the Dissect Team (<dissect@fox-it.com>) and made available at <https://github.com/fox-it/dissect>.

License terms: Apache License 2.0 (<https://www.apache.org/licenses/LICENSE-2.0>). For more information, see the LICENSE file.
