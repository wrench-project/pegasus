[![Build Status][travis-badge]][travis-link]

# WRENCH-Pegasus Implementation

<img src="https://raw.githubusercontent.com/wrench-project/wrench/master/doc/images/logo-vertical.png" width="100" />

This project is a WRENCH-based simulator of the [Pegasus](https://pegasus.isi.edu) WMS. 
Since Pegasus relies on HTCondor, first we have implemented the HTCondor services as

WRENCH is an _open-source library_ for developing workflow simulators. WRENCH exposes several high-level simulation 
abstractions to provide the **building blocks** for developing custom simulators.

More information: [WRENCH Project Website](http://wrench-project.org)

<img src="doc/images/wrench-pegasus-architecture.png" width="200" />

## Prerequisites

WRENCH-Pegasus is fully developed in C++. The code follows the C++11 standard, and thus older 
compilers tend to fail the compilation process. Therefore, we strongly recommend
users to satisfy the following requirements:

- **CMake** - version 3.2.3 or higher
  
And, one of the following:
- **g++** - version 5.0 or higher
- **clang** - version 3.6 or higher

## Dependencies

- [WRENCH](http://wrench-project.org/) - version [1.0.0](https://github.com/wrench-project/wrench/releases/tag/1.0.0)

## Building From Source

If all dependencies are installed, compiling and installing WRENCH-Pegasus is as simple as running:

```bash
cmake .
make
make install  # try "sudo make install" if you don't have the permission to write
```

## Get in Touch

The main channel to reach the WRENCH-Pegasus team is via the support email: 
[support@wrench-project.org](mailto:support@wrench-project.org).

**Bug Report / Feature Request:** our preferred channel to report a bug or request a feature is via  
WRENCH-Pegasus' [Github Issues Track](https://github.com/wrench-project/pegasus/issues).


[travis-badge]:             https://travis-ci.org/wrench-project/pegasus.svg?branch=master
[travis-link]:              https://travis-ci.org/wrench-project/pegasus

