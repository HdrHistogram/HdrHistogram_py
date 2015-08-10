========
Overview
========

High Dynamic Range Histogram pure python implementation

This repository contains a port to python of portions of the HDR Histogram
library:

- Basic histogram value recorging
    - record value
    - record value with correction for coordinated omission
- Supports 16-bit, 32-bit and 64-bit counters
- All histogram basic query APIs
    - get value at percentile
    - get total count
    - get min value, max value, mean, standard deviation
- All iterators are implemented: all values, recorded, percentile, linear, logarithmic
- Text file histogram log writer and log reader
- Histogram encoding and decoding (HdrHistogram V1 format only, V0 not supported)

Histogram V1 format encoding compatibility with Java and C versions verified through unit test code.

Acknowledgements
----------------

The python code was directly ported from the original HDR Histogram Java and C libraries:

* https://github.com/HdrHistogram/HdrHistogram.git
* https://github.com/HdrHistogram/HdrHistogram_c.git


Installation
------------
Pre-requisites:

Make sure you have python 2.7 and pip installed

Binary installation
^^^^^^^^^^^^^^^^^^^

.. code::

    pip install hdrhistogram

Source code installation and Unit Testing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install the unit test automation harness tox and hdrhistogram from github

.. code::

    pip install tox
    # cd to the proper location to clone the repository
    git clone https://github.com/ahothan/hdrhistogram.git
    cd hdrhistogram

Run the unit test using tox to execute:

- flake8 for syntax and indentation checking
- the python unit test code

The first run will take more time as tox will setup the execution environment and download the necessary packages:

.. code::

    $ tox
    GLOB sdist-make: /openstack/pyhdr/hdrhistogram/setup.py
    py27 inst-nodeps: /openstack/pyhdr/hdrhistogram/.tox/dist/hdrhistogram-0.1.0.zip
    py27 installed: flake8==2.4.1,hdrhistogram==0.1.0,mccabe==0.3.1,pep8==1.5.7,py==1.4.30,pyflakes==0.8.1,pytest==2.7.2,wsgiref==0.1.2
    py27 runtests: PYTHONHASHSEED='40561919'
    py27 runtests: commands[0] | py.test -q -s --basetemp=/openstack/pyhdr/hdrhistogram/.tox/py27/tmp
    
    ............................
    28 passed in 6.40 seconds
    
    pep8 inst-nodeps: /openstack/pyhdr/hdrhistogram/.tox/dist/hdrhistogram-0.1.0.zip
    pep8 installed: flake8==2.4.1,hdr-histogram==0.1,hdrhistogram==0.1.0,mccabe==0.3.1,pep8==1.5.7,py==1.4.30,pyflakes==0.8.1,pytest==2.7.2,wsgiref==0.1.2
    pep8 runtests: PYTHONHASHSEED='40561919'
    pep8 runtests: commands[0] | flake8 hdrh test
    ________________________________________________________________________ summary _________________________________________________________________________
      py27: commands succeeded
      pep8: commands succeeded
      congratulations :)
    $

Aggregation of Distributed Histograms
-------------------------------------

Aggregation of multiple histograms into 1 is useful in cases where tools
that generate these individual histograms have to run in a distributed way in
order to scale sufficiently.
As an example, the wrk2 tool (https://github.com/giltene/wrk2.git) is a great
tool for measuring the latency of HTTP requests with a large number of
connections. Although this tool can support thousands of connections per
process, some setups require massive scale in the order of hundreds of
thousands of connections which require running a large number of instances of
wrk processes, possibly on a large number of servers.
Given that each instance of wrk can generate a separate histogram, assessing
the scale of the entire system requires aggregating all these histograms
into 1 in a way that does not impact the accuracy of the results.
So there are 2 problems to solve:

- find a way to properly aggregate multiple histograms without losing any detail

- find a way to transport all these histograms into a central place

This library provides a solution for the aggregation part of the problem:

- reuse the HDR histogram compression format version 1 to encode and compress a complete histogram that can be sent over the wire to the aggregator

- provide python APIs to easily and efficiently:

    - compress an histogram instance into a transportable string
    - decompress a compressed histogram and add it to an existing histogram

Refer to the unit test code (test/test_hdrhistogram.py) to see how these APIs can be used.

Limitations and Caveats
-----------------------

The latest features and bug fixes of the original HDR histogram library may not be available in this python port.
List of notable features/APIs not implemented:

- concurrency support (AtomicHistogram, ConcurrentHistogram...)
- DoubleHistogram
- histogram auto-resize
- recorder function


Licensing
---------

This code is licensed under Apache License 2.0.
The original implementation in Java is licensed under CCO 1.0
(http://creativecommons.org/publicdomain/zero/1.0/)

Contribution
------------
External contribution and forks are welcome.

Changes can be contributed back using preferably GerritHub (https://review.gerrithub.io/#/q/project:ahothan/hdrhistogram)

GitHub pull requests can also be considered.


Links
-----

* Source: https://github.com/ahothan/hdrhistogram.git

