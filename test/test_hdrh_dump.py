#!/usr/bin/env python
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

from hdrh.histogram import HdrHistogram


def test_dump_histogram():
    samples = [
        # standard Hdr test histogram
        'HISTFAAAACF4nJNpmSzMwMDAzAABMJoRTM6Y1mD/ASLwN5oJAFuQBYU=',
        'HISTFAAAACh4nJNpmSzMwMDAyQABzFCaEUzOmNZg/wEisL2Kaasc00ImJgCC8Qbe'
    ]
    for hdrh in samples:
        with open(os.devnull, 'wb') as fnull:
            HdrHistogram.dump(hdrh, output=fnull)
        HdrHistogram.dump(hdrh)
