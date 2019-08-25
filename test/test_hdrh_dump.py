'''
Test code for the python version of HdrHistogram.

Ported from
https://github.com/HdrHistogram/HdrHistogram (Java)
https://github.com/HdrHistogram/HdrHistogram_c (C)

Written by Alec Hothan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import os
import sys

from hdrh.histogram import HdrHistogram


ENCODE_SAMPLES_HDRHISTOGRAM_C = [
    # standard Hdr test histogram
    'HISTFAAAACF4nJNpmSzMwMDAzAABMJoRTM6Y1mD/ASLwN5oJAFuQBYU=',
    'HISTFAAAACh4nJNpmSzMwMDAyQABzFCaEUzOmNZg/wEisL2Kaasc00ImJgCC8Qbe'
]

def dump_histogram(encoded_histogram):
    print('\nDumping histogram: ' + encoded_histogram)
    histogram = HdrHistogram.decode(encoded_histogram)

    histogram.output_percentile_distribution(open(os.devnull, 'wb'), 1)
    # sys.stdout.buffer will raise AttributeError in python 2.7
    try:
        # python 3 requires .buffer to write bytes
        output = sys.stdout.buffer
    except AttributeError:
        # in python 2.7, bytes can be writtent to sys.stdout
        output = sys.stdout
    histogram.output_percentile_distribution(output, 1)

def test_dump_histogram():
    for hdrh in ENCODE_SAMPLES_HDRHISTOGRAM_C:
        dump_histogram(hdrh)

def main():
    args = sys.argv[1:]
    if args:
        encoded_histograms = args
    else:
        encoded_histograms = ENCODE_SAMPLES_HDRHISTOGRAM_C

    for hdrh in encoded_histograms:
        dump_histogram(hdrh)


if __name__ == '__main__':
    main()
