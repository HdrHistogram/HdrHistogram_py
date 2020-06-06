#!/usr/bin/env python
'''
Utility to dump any hdrh histogram from encoded string

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
import sys

from hdrh.histogram import HdrHistogram

def dump(args=None):
    """
    Dump a list of Hdr histograms encodings

    args: list of strings, each string representing an Hdr encoding
    """
    if not args:
        args = sys.argv[1:]
    res = 1
    if args:
        encoded_histograms = args
        for hdrh in encoded_histograms:
            print('\nDumping histogram: ' + hdrh + '\n')
            HdrHistogram.dump(hdrh)
        res = 0
    else:
        print('\nUsage: %s [<string encoded hdr histogram>]*\n' % (sys.argv[0]))
    return res


if __name__ == '__main__':
    sys.exit(dump())
