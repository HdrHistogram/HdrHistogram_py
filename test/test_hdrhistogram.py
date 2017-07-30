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
from __future__ import division
from __future__ import print_function
from builtins import range
import cProfile
import datetime
import os
import zlib
import sys

from ctypes import addressof
from ctypes import c_uint8
from ctypes import c_uint16
from ctypes import c_uint32
from ctypes import c_uint64
from ctypes import sizeof
from ctypes import string_at

from hdrh.codec import HdrPayload
from hdrh.codec import HdrCookieException
from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogWriter
from hdrh.log import HistogramLogReader
from pyhdrh import add_array
from pyhdrh import encode
from pyhdrh import decode

import pytest

# histogram __init__ values
LOWEST = 1
HIGHEST = 3600 * 1000 * 1000
SIGNIFICANT = 3
TEST_VALUE_LEVEL = 4
INTERVAL = 10000

@pytest.mark.basic
def test_basic():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert histogram.bucket_count == 22
    assert histogram.sub_bucket_count == 2048
    assert histogram.counts_len == 23552
    assert histogram.unit_magnitude == 0
    assert histogram.sub_bucket_half_count_magnitude == 10

@pytest.mark.basic
def test_empty_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert histogram.get_min_value() == 0
    assert histogram.get_max_value() == 0
    assert histogram.get_mean_value() == 0
    assert histogram.get_stddev() == 0

@pytest.mark.basic
def test_large_numbers():
    histogram = HdrHistogram(20000000, 100000000, 5)
    histogram.record_value(100000000)
    histogram.record_value(20000000)
    histogram.record_value(30000000)
    assert histogram.values_are_equivalent(20000000, histogram.get_value_at_percentile(50.0))
    assert histogram.values_are_equivalent(30000000, histogram.get_value_at_percentile(83.33))
    assert histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(83.34))
    assert histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(99.0))

@pytest.mark.basic
def test_record_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    histogram.record_value(TEST_VALUE_LEVEL)
    assert histogram.get_count_at_value(TEST_VALUE_LEVEL) == 1
    assert histogram.get_total_count() == 1

@pytest.mark.basic
def test_highest_equivalent_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert 8183 * 1024 + 1023 == histogram.get_highest_equivalent_value(8180 * 1024)
    assert 8191 * 1024 + 1023 == histogram.get_highest_equivalent_value(8191 * 1024)
    assert 8199 * 1024 + 1023 == histogram.get_highest_equivalent_value(8193 * 1024)
    assert 9999 * 1024 + 1023 == histogram.get_highest_equivalent_value(9995 * 1024)
    assert 10007 * 1024 + 1023 == histogram.get_highest_equivalent_value(10007 * 1024)
    assert 10015 * 1024 + 1023 == histogram.get_highest_equivalent_value(10008 * 1024)

@pytest.mark.basic
def test_scaled_highest_equiv_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert histogram.get_highest_equivalent_value(8180) == 8183
    assert histogram.get_highest_equivalent_value(8191) == 8191
    assert histogram.get_highest_equivalent_value(8193) == 8199
    assert histogram.get_highest_equivalent_value(9995) == 9999
    assert histogram.get_highest_equivalent_value(10007) == 10007
    assert histogram.get_highest_equivalent_value(10008) == 10015

def load_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    # record this value with a count of 10,000
    histogram.record_value(1000, 10000)
    histogram.record_value(100000000)
    return histogram

def load_corrected_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    # record this value with a count of 10,000
    histogram.record_corrected_value(1000, INTERVAL, 10000)
    histogram.record_corrected_value(100000000, INTERVAL)
    return histogram

def check_percentile(hist, percentile, value, variation):
    value_at = hist.get_value_at_percentile(percentile)
    assert abs(value_at - value) < value * variation

def check_hist_percentiles(hist, total_count, perc_value_list):
    for pair in perc_value_list:
        check_percentile(hist, pair[0], pair[1], 0.001)
    assert hist.get_total_count() == total_count
    assert hist.values_are_equivalent(hist.get_min_value(), 1000.0)
    assert hist.values_are_equivalent(hist.get_max_value(), 100000000.0)

def check_percentiles(histogram, corrected_histogram):
    check_hist_percentiles(histogram,
                           10001,
                           ((30.0, 1000.0),
                            (99.0, 1000.0),
                            (99.99, 1000.0),
                            (99.999, 100000000.0),
                            (100.0, 100000000.0)))
    check_hist_percentiles(corrected_histogram,
                           20000,
                           ((30.0, 1000.0),
                            (50.0, 1000.0),
                            (75.0, 50000000.0),
                            (90.0, 80000000.0),
                            (99.0, 98000000.0),
                            (99.999, 100000000.0),
                            (100.0, 100000000.0)))

@pytest.mark.basic
def test_percentiles():
    check_percentiles(load_histogram(), load_corrected_histogram())

@pytest.mark.iterators
def test_recorded_iterator():
    hist = load_histogram()
    index = 0
    for item in hist.get_recorded_iterator():
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert count_added_in_this_bucket == 10000
        else:
            assert count_added_in_this_bucket == 1
        index += 1
    assert index == 2

    hist = load_corrected_histogram()
    index = 0
    total_added_count = 0
    for item in hist.get_recorded_iterator():
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert count_added_in_this_bucket == 10000

        assert item.count_at_value_iterated_to != 0
        total_added_count += count_added_in_this_bucket
        index += 1
    assert total_added_count == 20000
    assert total_added_count == hist.get_total_count()

def check_iterator_values(itr, last_index):
    index = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert count_added_in_this_bucket == 10000
        elif index == last_index:
            assert count_added_in_this_bucket == 1
        else:
            assert count_added_in_this_bucket == 0
        index += 1
    assert index - 1 == last_index

def check_corrected_iterator_values(itr, last_index):
    index = 0
    total_added_count = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            # first bucket is range [0, 10000]
            # value 1000  count = 10000
            # value 10000 count = 1 (corrected from the 100M value with 10K interval)
            assert count_added_in_this_bucket == 10001
        index += 1
        total_added_count += count_added_in_this_bucket

    assert index - 1 == last_index
    assert total_added_count == 20000

@pytest.mark.iterators
def test_linear_iterator():
    hist = load_histogram()
    itr = hist.get_linear_iterator(100000)
    check_iterator_values(itr, 999)
    hist = load_corrected_histogram()
    itr = hist.get_linear_iterator(10000)
    check_corrected_iterator_values(itr, 9999)

@pytest.mark.iterators
def test_log_iterator():
    hist = load_histogram()
    itr = hist.get_log_iterator(10000, 2.0)
    check_iterator_values(itr, 14)
    hist = load_corrected_histogram()
    itr = hist.get_log_iterator(10000, 2.0)
    check_corrected_iterator_values(itr, 14)

@pytest.mark.iterators
def test_percentile_iterator():
    hist = load_histogram()
    # test with 5 ticks per half distance
    for item in hist.get_percentile_iterator(5):
        expected = hist.get_highest_equivalent_value(hist.get_value_at_percentile(item.percentile))
        assert item.value_iterated_to == expected

@pytest.mark.iterators
def test_reset_iterator():
    hist = load_corrected_histogram()
    itr = hist.get_recorded_iterator()

    # do a partial iteration
    index = 0
    total_added_count = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert count_added_in_this_bucket == 10000

        assert item.count_at_value_iterated_to != 0
        total_added_count += count_added_in_this_bucket
        index += 1
        if total_added_count >= 10000:
            break

    # reset iterator and do a full iteration
    itr.reset()
    index = 0
    total_added_count = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert count_added_in_this_bucket == 10000

        assert item.count_at_value_iterated_to != 0
        total_added_count += count_added_in_this_bucket
        index += 1
    assert total_added_count == 20000
    assert total_added_count == hist.get_total_count()

    # just run the reset method
    hist.get_all_values_iterator().reset()
    hist.get_linear_iterator(100000).reset()
    hist.get_log_iterator(10000, 2.0).reset()
    hist.get_percentile_iterator(5).reset()

@pytest.mark.basic
def test_reset():
    histogram = load_histogram()
    histogram.reset()
    assert histogram.get_total_count() == 0
    assert histogram.get_value_at_percentile(99.99) == 0
    assert histogram.get_start_time_stamp() == sys.maxsize
    assert histogram.get_end_time_stamp() == 0

@pytest.mark.basic
def test_invalid_significant_figures():
    try:
        HdrHistogram(LOWEST, HIGHEST, -1)
        assert False
    except ValueError:
        pass
    try:
        HdrHistogram(LOWEST, HIGHEST, 6)
        assert False
    except ValueError:
        pass

@pytest.mark.basic
def test_out_of_range_values():
    histogram = HdrHistogram(1, 1000, 4)
    assert histogram.record_value(32767)
    assert histogram.record_value(32768) is False

# Make up a list of values for testing purpose
VALUES_LIST = (
    1000,
    1000,
    3000,
    3000
)

@pytest.mark.basic
def test_mean_stddev():
    # fill up a histogram with the values in the list
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    for value in VALUES_LIST:
        histogram.record_value(value)
    assert histogram.get_mean_value() == 2000.5
    assert histogram.get_stddev() == 1000.5

HDR_PAYLOAD_COUNTS = 1000
HDR_PAYLOAD_PARTIAL_COUNTS = HDR_PAYLOAD_COUNTS // 2

def fill_counts(payload, last_index, start=0):
    # note that this function should only be used for
    # raw payload level operations, shoud not be used for payloads that are
    # created from a histogram, see fill_hist_counts
    counts = payload.get_counts()
    for index in range(start, last_index):
        counts[index] = index

def check_counts(payload, last_index, multiplier=1, start=0):
    counts = payload.get_counts()
    for index in range(start, last_index):
        assert counts[index] == multiplier * index

def check_hdr_payload(counter_size):
    # Create an HdrPayload class with given counters count
    payload = HdrPayload(counter_size, HDR_PAYLOAD_COUNTS)
    # put some known numbers in the buckets
    fill_counts(payload, HDR_PAYLOAD_COUNTS)

    # get a compressed version of that payload
    cpayload = payload.compress(HDR_PAYLOAD_COUNTS)
    # now decompress it into a new hdr payload instance
    dpayload = HdrPayload(counter_size, compressed_payload=cpayload)
    dpayload.init_counts(HDR_PAYLOAD_COUNTS)

    # now verify that the counters are identical to the original
    check_counts(dpayload, HDR_PAYLOAD_COUNTS)

@pytest.mark.codec
def test_hdr_payload():
    # Check the payload work in all 3 supported counter sizes
    for counter_size in [2, 4, 8]:
        check_hdr_payload(counter_size)

@pytest.mark.codec
def test_hdr_payload_exceptions():
    # test invalid zlib compressed buffer
    with pytest.raises(zlib.error):
        HdrPayload(2, compressed_payload=b'junk data')

    # unsupported word size
    with pytest.raises(ValueError):
        payload = HdrPayload(1, HDR_PAYLOAD_COUNTS)
    with pytest.raises(ValueError):
        payload = HdrPayload(1000, HDR_PAYLOAD_COUNTS)

    # invalid cookie
    payload = HdrPayload(8, HDR_PAYLOAD_COUNTS)
    payload.payload.cookie = 12345
    cpayload = payload.compress(HDR_PAYLOAD_COUNTS)
    with pytest.raises(HdrCookieException):
        HdrPayload(2, compressed_payload=cpayload)

def fill_hist_counts(histogram, last_index, start=0):
    # fill the counts of a given histogram and update the min/max/total count
    # accordingly
    for index in range(start, last_index):
        value_from_index = histogram.get_value_from_index(index)
        histogram.record_value(value_from_index, index)

def check_hist_counts(histogram, last_index, multiplier=1, start=0):
    for index in range(start, last_index):
        assert histogram.get_count_at_index(index) == multiplier * index

# This is the max latency used by wrk2
WRK2_MAX_LATENCY = 24 * 60 * 60 * 1000000

def check_hist_encode(word_size,
                      digits,
                      expected_compressed_length,
                      fill_start_percent,
                      fill_count_percent):
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, digits,
                             word_size=word_size)
    if fill_count_percent:
        fill_start_index = (fill_start_percent * histogram.counts_len) // 100
        fill_to_index = fill_start_index + (fill_count_percent * histogram.counts_len) // 100
        fill_hist_counts(histogram, fill_to_index, fill_start_index)
    b64 = histogram.encode()
    assert len(b64) == expected_compressed_length

# A list of call arguments to check_hdr_encode
ENCODE_ARG_LIST = (
    # word size digits  expected_compressed_length, fill_start%, fill_count%
    # best case when all counters are zero
    (8, 3, 48, 0, 0),        # V1=52 385 = size when compressing entire counts array
    (8, 2, 48, 0, 0),        # 126
    # typical case when all counters are aggregated in a small contiguous area
    (8, 3, 15560, 30, 20),   # V1=16452
    (8, 2, 1688, 30, 20),    # V1=2096
    # worst case when all counters are different
    (8, 3, 76892, 0, 100),   # V1=80680
    (8, 2, 9340, 0, 100),    # V1=10744
    # worst case 32-bit and 16-bit counters
    (2, 3, 76892, 0, 100),   # V1=68936
    (2, 2, 9340, 0, 100),    # V1=9144
)

@pytest.mark.codec
def test_hist_encode():
    for args in ENCODE_ARG_LIST:
        check_hist_encode(*args)

@pytest.mark.codec
def check_hist_codec_b64(word_size, b64_wrap):
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, SIGNIFICANT,
                             b64_wrap=b64_wrap,
                             word_size=word_size)
    # encode with all zero counters
    encoded = histogram.encode()
    # add back same histogram
    histogram.decode_and_add(encoded)
    # counters should remain zero
    check_hist_counts(histogram, histogram.counts_len, multiplier=0)
    # fill up the histogram
    fill_hist_counts(histogram, histogram.counts_len)
    encoded = histogram.encode()
    histogram.decode_and_add(encoded)
    check_hist_counts(histogram, histogram.counts_len, multiplier=2)

@pytest.mark.codec
def test_hist_codec():
    for word_size in [2, 4, 8]:
        check_hist_codec_b64(word_size, True)
        check_hist_codec_b64(word_size, False)

@pytest.mark.codec
def test_hist_codec_partial():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, SIGNIFICANT)

    partial_histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, SIGNIFICANT)

    # put some known numbers in the first half buckets
    half_count = partial_histogram.counts_len
    fill_hist_counts(partial_histogram, half_count)
    encoded = partial_histogram.encode()
    histogram.decode_and_add(encoded)

    # now verify that the partial counters are identical to the original
    check_hist_counts(histogram, half_count, multiplier=1)
    check_hist_counts(histogram, histogram.counts_len, start=half_count + 1, multiplier=0)

# A list of encoded histograms as generated by the test code in HdrHistogram_c
# encoded from the standard Hdr test histograms (load_histogram())
# These are all histograms with 64-bit counters

ENCODE_SAMPLES_HDRHISTOGRAM_C = [
    # standard Hdr test histogram
    'HISTFAAAACl4nJNpmSzMwMBgyAABzFCaEURcm7yEwf4DROA8/4I5jNM7mJgAlWkH9g==',
    # standard Hdr test corrected histogram
    'HISTFAAAAP94nJNpmSzMwCByigECmKE0I4i4NnkJg/0HiMB5/gVzGD8aM/3lZ7rPyTSbjektC9N7Fqa'
    'HzEzbmZi2whCEvZKRaSYj02wwiYng4tFM3lDoC2dhhwh5UyZlJlUMjClCmgpMEUUmQSZ+IBZEojFFCM'
    'vQRwUxenmZ2MGQFUqz4+CTo4I2pg4dFdQylZWJkYkZCaPyMEUIydPLjMHrssFixuB12XD0HRAwMsFJg'
    'kwilZHJHHKmjzp41MFDOjhYmFiQEUEmMWqGsvKBd8Fwd/Co/waTC9jYOMAIiSJRhLbKh5x9Q87Bo/YN'
    'bfsoM4CPhw+IIJAkxnDXN+QcTIpyAPnGh6k='
]

@pytest.mark.codec
def test_hdr_interop():
    # decode and add the encoded histograms
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    corrected_histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    histogram.decode_and_add(ENCODE_SAMPLES_HDRHISTOGRAM_C[0])
    corrected_histogram.decode_and_add(ENCODE_SAMPLES_HDRHISTOGRAM_C[1])

    # check the percentiles. min, max values match
    check_percentiles(histogram, corrected_histogram)

def check_cod_perf():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, 2)
    fill_start_index = (20 * histogram.counts_len) // 100
    fill_to_index = fill_start_index + (30 * histogram.counts_len) // 100
    fill_hist_counts(histogram, fill_to_index, fill_start_index)

    # encode 1000 times
    start = datetime.datetime.now()
    for _ in range(1000):
        histogram.encode()
    delta = datetime.datetime.now() - start
    print(delta)

def check_dec_perf():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, 2)
    fill_start_index = (20 * histogram.counts_len) // 100
    fill_to_index = fill_start_index + (30 * histogram.counts_len) // 100
    fill_hist_counts(histogram, fill_to_index, fill_start_index)
    b64 = histogram.encode()

    # decode and add to self 1000 times
    start = datetime.datetime.now()
    for _ in range(1000):
        histogram.decode_and_add(b64)
    delta = datetime.datetime.now() - start
    print(delta)

@pytest.mark.perf
def test_cod_perf():
    cProfile.runctx('check_cod_perf()', globals(), locals())

@pytest.mark.perf
def test_dec_perf():
    cProfile.runctx('check_dec_perf()', globals(), locals())

def check_decoded_hist_counts(hist, multiplier):
    assert hist
    check_hist_counts(hist, hist.counts_len, multiplier)

HDR_LOG_NAME = 'hdr.log'
@pytest.mark.log
def test_log():
    # 3 histograms instances with various content
    empty_hist = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    hist = load_histogram()
    corrected_hist = load_corrected_histogram()
    with open(HDR_LOG_NAME, 'w') as hdr_log:
        log_writer = HistogramLogWriter(hdr_log)
        log_writer.output_comment("Logged with hdrhistogram.py")
        log_writer.output_log_format_version()
        log_writer.output_legend()
        # snapshot the 3 histograms
        log_writer.output_interval_histogram(empty_hist)
        log_writer.output_interval_histogram(hist)
        log_writer.output_interval_histogram(corrected_hist)
        log_writer.close()

    # decode the log file and check the decoded histograms
    log_reader = HistogramLogReader(HDR_LOG_NAME, empty_hist)
    decoded_empty_hist = log_reader.get_next_interval_histogram()
    check_decoded_hist_counts(decoded_empty_hist, 0)
    decoded_hist = log_reader.get_next_interval_histogram()
    decoded_corrected_hist = log_reader.get_next_interval_histogram()
    check_percentiles(decoded_hist, decoded_corrected_hist)
    assert log_reader.get_next_interval_histogram() is None

JHICCUP_V2_LOG_NAME = "test/jHiccup-2.0.7S.logV2.hlog"
# Test input and expected output values
JHICCUP_CHECKLISTS = [
    {'target': {'histogram_count': 62,
                'total_count': 48761,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1745879039,
                'accumulated_histogram.get_max_value()': 1796210687,
                'log_reader.get_start_time_sec()': 1441812279.474}},
    {'range_start_time_sec': 5,
     'range_end_time_sec': 20,
     'target': {'histogram_count': 15,
                'total_count': 11664,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1536163839,
                'accumulated_histogram.get_max_value()': 1544552447}},
    {'range_start_time_sec': 40,
     'range_end_time_sec': 60,
     'target': {'histogram_count': 20,
                'total_count': 15830,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1779433471,
                'accumulated_histogram.get_max_value()': 1796210687}}
]

@pytest.mark.log
def test_jHiccup_v2_log():
    accumulated_histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    for checklist in JHICCUP_CHECKLISTS:
        accumulated_histogram.reset()
        log_reader = HistogramLogReader(JHICCUP_V2_LOG_NAME, accumulated_histogram)

        histogram_count = 0
        total_count = 0
        target_numbers = checklist.pop('target')
        while 1:
            decoded_histogram = log_reader.get_next_interval_histogram(**checklist)
            if not decoded_histogram:
                break
            histogram_count += 1
            total_count += decoded_histogram.get_total_count()
            accumulated_histogram.add(decoded_histogram)
            # These logs use 8 byte counters
            assert decoded_histogram.get_word_size() == 8
        for statement in target_numbers:
            assert eval(statement) == target_numbers[statement]

        log_reader.close()

@pytest.mark.log
def test_output_percentile_distribution():
    histogram = load_histogram()
    histogram.output_percentile_distribution(open(os.devnull, 'wb'), 1000)

ARRAY_SIZE = 10

@pytest.mark.pyhdrh
def test_add_array_errors():
    with pytest.raises(TypeError):
        add_array()
    with pytest.raises(TypeError):
        add_array(100)
    with pytest.raises(TypeError):
        add_array(None, None, 0, 0)
    src_array = (c_uint16 * ARRAY_SIZE)()
    # negative length
    with pytest.raises(ValueError):
        add_array(addressof(src_array), addressof(src_array), -1, sizeof(c_uint16))
    # invalid word size
    with pytest.raises(ValueError):
        add_array(addressof(src_array), addressof(src_array), 0, 0)

def check_add_array(int_type):
    src_array = (int_type * ARRAY_SIZE)()
    dst_array = (int_type * ARRAY_SIZE)()
    expect_added = 0
    for index in range(ARRAY_SIZE):
        src_array[index] = index
        expect_added += index
    added = add_array(addressof(dst_array), addressof(src_array), ARRAY_SIZE, sizeof(int_type))
    assert added == expect_added
    for index in range(ARRAY_SIZE):
        assert dst_array[index] == index
    # overflow
    src_array[0] = -1
    dst_array[0] = -1
    with pytest.raises(OverflowError):
        add_array(addressof(dst_array), addressof(src_array), ARRAY_SIZE, sizeof(int_type))

@pytest.mark.pyhdrh
def test_add_array():
    for int_type in [c_uint16, c_uint32, c_uint64]:
        check_add_array(int_type)

@pytest.mark.pyhdrh
def test_zz_encode_errors():
    with pytest.raises(TypeError):
        encode()
    with pytest.raises(TypeError):
        encode(None, None, 0, 0)
    src_array = (c_uint16 * ARRAY_SIZE)()
    src_array_addr = addressof(src_array)
    dst_len = 9 * ARRAY_SIZE

    # negative length
    with pytest.raises(ValueError):
        encode(src_array_addr, -1, sizeof(c_uint16), 0, dst_len)
    # dest length too small
    with pytest.raises(ValueError):
        encode(src_array_addr, ARRAY_SIZE, 4, 0, 4)
    # invalid word size
    with pytest.raises(ValueError):
        encode(src_array_addr, ARRAY_SIZE, 3, 0, 0)
    # Null dest ptr
    with pytest.raises(ValueError):
        encode(src_array_addr, ARRAY_SIZE, 4, 0, dst_len)

def check_zz_encode(int_type):
    src_array = (int_type * ARRAY_SIZE)()
    src_array_addr = addressof(src_array)
    dst_len = 9 * ARRAY_SIZE
    dst_array = (c_uint8 * dst_len)()
    dst_array_addr = addressof(dst_array)

    res = encode(src_array_addr, ARRAY_SIZE, sizeof(int_type), dst_array_addr, dst_len)
    # should be 1 byte set to 0x13 (10 zeros => value = -10, or 0x13 in zigzag
    # encoding
    assert res == 1
    assert dst_array[0] == 0x13

    # last counter set to 1
    # the encoded result should be 2 bytes long
    # 0x11   (9 zeros => -9 coded as 17)
    # 0x02   (1 is coded as 2)
    src_array[ARRAY_SIZE - 1] = 1
    res = encode(src_array_addr, ARRAY_SIZE, sizeof(int_type), dst_array_addr, dst_len)
    assert res == 2
    assert dst_array[0] == 0x11
    assert dst_array[1] == 0x02

    # all counters set to 1, we should get a zigzag encoded of
    # 10 bytes all set to 0x02 (in zigzag encoding 1 is coded as 2)
    for index in range(ARRAY_SIZE):
        src_array[index] = 1
    res = encode(src_array_addr, ARRAY_SIZE, sizeof(int_type), dst_array_addr, dst_len)
    assert res == ARRAY_SIZE
    for index in range(ARRAY_SIZE):
        assert dst_array[index] == 2

@pytest.mark.pyhdrh
def test_zz_encode():
    for int_type in [c_uint16, c_uint32, c_uint64]:
        check_zz_encode(int_type)

# Few malicious V2 encodes using ZiZag LEB128/9 bytes
# Valid large value overflows smaller size dest counter
# This is the largest positive number (zigzag odd numbers are positive)
LARGE_POSITIVE_VALUE = b'\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
# This is the largest negative number
LARGE_NEGATIVE_VALUE = b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
#
# A simple 1 at index 0, followed by a
# large enough negative value to be dangerous: -2147483648 (smallest negative signed 32 bit)
INDEX_SKIPPER_VALUE = b'\x01\x02\xFF\xFF\xFF\xFF\x0F\x02'
# Truncated end
TRUNCATED_VALUE = b'\xFF\xFF'

@pytest.mark.pyhdrh
def test_zz_decode_errors():
    with pytest.raises(TypeError):
        decode(None, None, 0, 0)
    dst_array = (c_uint16 * ARRAY_SIZE)()
    # negative array size
    with pytest.raises(IndexError):
        decode(b' ', 0, addressof(dst_array), -1, sizeof(c_uint16))
    # invalid word size
    with pytest.raises(ValueError):
        decode(b' ', 0, addressof(dst_array), ARRAY_SIZE, 3)
    # read index negative
    with pytest.raises(IndexError):
        decode(b'', -1, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    # Truncated end
    with pytest.raises(ValueError):
        decode(TRUNCATED_VALUE, 0, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    # Too large positive value for this counter size
    with pytest.raises(OverflowError):
        decode(LARGE_POSITIVE_VALUE, 0, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    # Negative overflow
    with pytest.raises(OverflowError):
        decode(LARGE_NEGATIVE_VALUE, 0, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    # zero count skip index out of bounds
    with pytest.raises(IndexError):
        decode(INDEX_SKIPPER_VALUE, 0, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    # read index too large => empty results
    res = decode(b'BUMMER', 8, addressof(dst_array), ARRAY_SIZE, sizeof(c_uint16))
    assert res['total'] == 0

def check_zz_identity(src_array, int_type, min_nz_index, max_nz_index, total_count, offset):
    dst_len = (sizeof(int_type) + 1) * ARRAY_SIZE
    dst = (c_uint8 * (offset + dst_len))()

    varint_len = encode(addressof(src_array), ARRAY_SIZE, sizeof(int_type),
                        addressof(dst) + offset, dst_len)
    varint_string = string_at(dst, varint_len + offset)

    dst_array = (int_type * ARRAY_SIZE)()
    res = decode(varint_string, offset, addressof(dst_array), ARRAY_SIZE, sizeof(int_type))
    assert res['total'] == total_count
    if total_count:
        assert res['min_nonzero_index'] == min_nz_index
        assert res['max_nonzero_index'] == max_nz_index
    for index in range(ARRAY_SIZE):
        assert dst_array[index] == src_array[index]

# A large positive value that can fit 16-bit signed
ZZ_COUNTER_VALUE = 30000

def check_zz_decode(int_type, hdr_len):
    src_array = (int_type * ARRAY_SIZE)()
    check_zz_identity(src_array, int_type, 0, 0, 0, hdr_len)

    # last counter set to ZZ_COUNTER_VALUE
    # min=max=ARRAY_SIZE-1
    src_array[ARRAY_SIZE - 1] = ZZ_COUNTER_VALUE
    check_zz_identity(src_array, int_type, ARRAY_SIZE - 1,
                      ARRAY_SIZE - 1, ZZ_COUNTER_VALUE, hdr_len)

    # all counters set to ZZ_COUNTER_VALUE
    for index in range(ARRAY_SIZE):
        src_array[index] = ZZ_COUNTER_VALUE
    check_zz_identity(src_array, int_type, 0, ARRAY_SIZE - 1,
                      ZZ_COUNTER_VALUE * ARRAY_SIZE, hdr_len)

@pytest.mark.pyhdrh
def test_zz_decode():
    for int_type in [c_uint16, c_uint32, c_uint64]:
        for hdr_len in [0, 8]:
            check_zz_decode(int_type, hdr_len)

def hex_dump(label, str):
    print(label)
    print(':'.join(x.encode('hex') for x in str))

@pytest.mark.basic
def test_get_value_at_percentile():
    histogram = HdrHistogram(LOWEST, 3600000000L, 3)
    histogram.record_value(1)
    histogram.record_value(2)
    assert histogram.get_value_at_percentile(50.0) == 1
    assert histogram.get_value_at_percentile(50.00000000000001) == 1
    # assert histogram.get_value_at_percentile(50.0000000000001) == 2
    histogram.record_value(2)
    histogram.record_value(2)
    histogram.record_value(2)
    # val = histogram.get_value_at_percentile(25)
    # assert histogram.get_value_at_percentile(25) == 2
    assert histogram.get_value_at_percentile(30) == 2

