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

Simply aggregating a summary such as a set of latencies at percentile list is not sufficient because
it is not correct to simply average all percentile values coming from all the sources.
If we take the simple example of 2 sources that each produce the following results:

Source 1:

- 20 seconds at 50 percentile

- 21 seconds at 90 percentile

Source 2:

- 4 seconds at 50 percentile

- 5 seconds at 90 percentile

A naive aggregation would be to average the 2 sets of values and end up with:

- (20+4)/2 = 12 seconds at 50 percentile

- (21+5)/2 = 13 seconds at 90 percentile

The mere fact that a 21 second latency at 90 percentile has been reduced to 13 seconds is a glaring proof that averaging values at same latency is incorrect.
Weighting each set by the number of total samples is not correct either.



Proposed Solution
^^^^^^^^^^^^^^^^^
As the above simple example may have hinted, the only correct way of aggregating multiple histograms
is to add same-bucket counters, assuming all histograms have the same bucket structure
(meaning same dynamic range and same precision digits).
This solution defines a description of the information needed from each histogram source in order to aggregate all of them into 1 aggregate histogram that represents faithfully the state of the entire system.
Each source must send the list of all bucket/sub-bucket counts so the aggregator can add all the counts.

Because the number of empty buckets generally vastly outnumber non-zero buckets, it is interesting to only
forward non-zero buckets in order to minimize the amount of information transferred.
As an example, a typical run with 27 buckets x 2048 sub-buckets (which corresponds to the default wrk2 configuration) yields about 100 non-zero buckets.


Histogram JSON Format
^^^^^^^^^^^^^^^^^^^^^
Example of JSON document:
.. code::
    {
        "buckets": 27,
        "sub_buckets": 2048,
        "digits": 3,
        "max_latency": 86400000000,
        "min": 89151,
        "max": 209664,
        "counters": [
            6, [1295, 1, 1392, 1, 1432, 1, 1435, 1, 1489, 1, 1493, 1, 1536, 1, 1553, 1,
                1560, 1, 1574, 1, 1591, 1, 1615, 1, 1672, 1, 1706, 1, 1738, 1, 1812, 1,
                1896, 1],
            7, [1559, 1, 1590, 1, 1638, 1]]
    }

The "counters" value is a list of bucket index and list of sub-bucket index, counter values.


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

