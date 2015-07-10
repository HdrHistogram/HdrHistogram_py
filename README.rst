========
Overview
========

High Dynamic Range Histogram python library

This repository contains a port/rewrite in python of portions of the HDR Histogram
library augmented with a few extensions to support an accurate aggregation of
distributed histograms (see details below).


Acknowledgements
----------------

The python code was directly inspired from the HDR Histogram C library
that was residing in the github wrk2 repository:
https://github.com/giltene/wrk2/blob/master/src/hdr_histogram.c

The original HDR Histogram in Java is from Gil Tene:
https://github.com/HdrHistogram/HdrHistogram.git

The python test code was copied from portions of the C and Java implementation
https://github.com/HdrHistogram/HdrHistogram_c.git


Aggregation of Distributed Histograms
-------------------------------------

Problem Statement
^^^^^^^^^^^^^^^^^
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

This library only provides a solution for the aggregation part of the problem:

- propose a format describing what histogram information needs to be sent to the aggregator

- provide python APIs to properly aggregate all these histograms into 1


Proposed Solution
^^^^^^^^^^^^^^^^^

Histogram JSON Format
^^^^^^^^^^^^^^^^^^^^^


Testing
-------

You need tox to be installed.
Just run tox from the repository top folder to execute:

- flake8 for syntax and indentation checking

- the python unit test code


Limitations and Caveats
-----------------------

The latest features and bug fixes of the original HDR histogram library are
likely not available in this python port.

Licensing
---------

This code is licensed under Apache License 2.0.
The original implementation in Java (https://github.com/giltene/wrk2.git) is licensed under CCO 1.0 (http://creativecommons.org/publicdomain/zero/1.0/)

Contribution
------------
External contribution and forks are welcome.

Changes can be contributed back using preferably GerritHub (https://review.gerrithub.io/#/q/project:ahothan/hdrhistogram)

GitHub pull requests can also be considered.


Links
-----

* Source: https://github.com/ahothan/hdrhistogram.git

