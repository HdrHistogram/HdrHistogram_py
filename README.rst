========
Overview
========

High Dynamic Range Histogram pure python implementation

This repository contains a port to python of portions of the HDR Histogram
library


Acknowledgements
----------------

The python code was directly inspired from the HDR Histogram C library
that was residing in the github wrk2 repository:
https://github.com/giltene/wrk2/blob/master/src/hdr_histogram.c

The original HDR Histogram in Java and C:
https://github.com/HdrHistogram/HdrHistogram.git
https://github.com/HdrHistogram/HdrHistogram_c.git


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
    py27 inst-nodeps: /openstack/pyhdr/hdrhistogram/.tox/dist/hdrhistogram-0.0.4.zip
    py27 installed: flake8==2.4.1,hdrhistogram==0.0.4,mccabe==0.3.1,pep8==1.5.7,py==1.4.30,pyflakes==0.8.1,pytest==2.7.2,wsgiref==0.1.2
    py27 runtests: PYTHONHASHSEED='311216085'
    py27 runtests: commands[0] | py.test -q -s --basetemp=/openstack/pyhdr/hdrhistogram/.tox/py27/tmp
    .........................
    25 passed in 3.22 seconds
    pep8 inst-nodeps: /openstack/pyhdr/hdrhistogram/.tox/dist/hdrhistogram-0.0.4.zip
    pep8 installed: flake8==2.4.1,hdr-histogram==0.1,hdrhistogram==0.0.4,mccabe==0.3.1,pep8==1.5.7,py==1.4.30,pyflakes==0.8.1,pytest==2.7.2,wsgiref==0.1.2
    pep8 runtests: PYTHONHASHSEED='311216085'
    pep8 runtests: commands[0] | flake8 hdrh test
    ___________________________________________________________________ summary ____________________________________________________________________
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
Notable features/APIs not yet implemented:

- concurrency support (AtomicHistogram, ConcurrentHistogram...)
- histogram auto-resize
- recorder

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

