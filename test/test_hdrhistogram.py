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
import cProfile
import datetime
import pytest
import zlib

from hdrh.codec import HdrPayload
from hdrh.codec import HdrCookieException
from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogWriter
from hdrh.log import HistogramLogReader


# histogram __init__ values
LOWEST = 1
HIGHEST = 3600 * 1000 * 1000
SIGNIFICANT = 3
TEST_VALUE_LEVEL = 4
INTERVAL = 10000

@pytest.mark.basic
def test_basic():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert(histogram.bucket_count == 22)
    assert(histogram.sub_bucket_count == 2048)
    assert(histogram.counts_len == 23552)
    assert(histogram.unit_magnitude == 0)
    assert(histogram.sub_bucket_half_count_magnitude == 10)

@pytest.mark.basic
def test_empty_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert(histogram.get_min_value() == 0)
    assert(histogram.get_max_value() == 0)
    assert(histogram.get_mean_value() == 0)
    assert(histogram.get_stddev() == 0)

@pytest.mark.basic
def test_large_numbers():
    histogram = HdrHistogram(20000000, 100000000, 5)
    histogram.record_value(100000000)
    histogram.record_value(20000000)
    histogram.record_value(30000000)
    assert(histogram.values_are_equivalent(20000000, histogram.get_value_at_percentile(50.0)))
    assert(histogram.values_are_equivalent(30000000, histogram.get_value_at_percentile(83.33)))
    assert(histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(83.34)))
    assert(histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(99.0)))

@pytest.mark.basic
def test_record_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    histogram.record_value(TEST_VALUE_LEVEL)
    assert(histogram.get_count_at_value(TEST_VALUE_LEVEL) == 1)
    assert(histogram.get_total_count() == 1)

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
    assert 8183 == histogram.get_highest_equivalent_value(8180)
    assert 8191 == histogram.get_highest_equivalent_value(8191)
    assert 8199 == histogram.get_highest_equivalent_value(8193)
    assert 9999 == histogram.get_highest_equivalent_value(9995)
    assert 10007 == histogram.get_highest_equivalent_value(10007)
    assert 10015 == histogram.get_highest_equivalent_value(10008)

def load_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    # record this value with a count of 10,000
    histogram.record_value(1000L, 10000)
    histogram.record_value(100000000L)
    return histogram

def load_corrected_histogram():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    # record this value with a count of 10,000
    histogram.record_corrected_value(1000L, INTERVAL, 10000)
    histogram.record_corrected_value(100000000L, INTERVAL)
    return histogram

def check_percentile(hist, percentile, value, variation):
    value_at = hist.get_value_at_percentile(percentile)
    assert(abs(value_at - value) < value * variation)

def check_hist_percentiles(hist, total_count, perc_value_list):
    for pair in perc_value_list:
        check_percentile(hist, pair[0], pair[1], 0.001)
    assert(hist.get_total_count() == total_count)
    assert(hist.values_are_equivalent(hist.get_min_value(), 1000.0))
    assert(hist.values_are_equivalent(hist.get_max_value(), 100000000.0))

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
            assert(count_added_in_this_bucket == 10000)
        else:
            assert(count_added_in_this_bucket == 1)
        index += 1
    assert(index == 2)

    hist = load_corrected_histogram()
    index = 0
    total_added_count = 0
    for item in hist.get_recorded_iterator():
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert(count_added_in_this_bucket == 10000)

        assert(item.count_at_value_iterated_to != 0)
        total_added_count += count_added_in_this_bucket
        index += 1
    assert(total_added_count == 20000)
    assert(total_added_count == hist.get_total_count())

def check_iterator_values(itr, last_index):
    index = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            assert(count_added_in_this_bucket == 10000)
        elif index == last_index:
            assert(count_added_in_this_bucket == 1)
        else:
            assert(count_added_in_this_bucket == 0)
        index += 1
    assert(index - 1 == last_index)

def check_corrected_iterator_values(itr, last_index):
    index = 0
    total_added_count = 0
    for item in itr:
        count_added_in_this_bucket = item.count_added_in_this_iter_step
        if index == 0:
            # first bucket is range [0, 10000]
            # value 1000  count = 10000
            # value 10000 count = 1 (corrected from the 100M value with 10K interval)
            assert(count_added_in_this_bucket == 10001)
        index += 1
        total_added_count += count_added_in_this_bucket

    assert(index - 1 == last_index)
    assert(total_added_count == 20000)

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
        assert(item.value_iterated_to == expected)

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
            assert(count_added_in_this_bucket == 10000)

        assert(item.count_at_value_iterated_to != 0)
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
            assert(count_added_in_this_bucket == 10000)

        assert(item.count_at_value_iterated_to != 0)
        total_added_count += count_added_in_this_bucket
        index += 1
    assert(total_added_count == 20000)
    assert(total_added_count == hist.get_total_count())

    # just run the reset method
    hist.get_all_values_iterator().reset()
    hist.get_linear_iterator(100000).reset()
    hist.get_log_iterator(10000, 2.0).reset()
    hist.get_percentile_iterator(5).reset()

@pytest.mark.basic
def test_reset():
    histogram = load_histogram()
    histogram.reset()
    assert(histogram.get_total_count() == 0)
    assert(histogram.get_value_at_percentile(99.99) == 0)

@pytest.mark.basic
def test_invalid_significant_figures():
    try:
        HdrHistogram(LOWEST, HIGHEST, -1)
        assert(False)
    except ValueError:
        pass
    try:
        HdrHistogram(LOWEST, HIGHEST, 6)
        assert(False)
    except ValueError:
        pass

@pytest.mark.basic
def test_out_of_range_values():
    histogram = HdrHistogram(1, 1000, 4)
    assert(histogram.record_value(32767))
    assert(histogram.record_value(32768) is False)

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
    assert(histogram.get_mean_value() == 2000.5)
    assert(histogram.get_stddev() == 1000.5)

HDR_PAYLOAD_COUNTS = 1000
HDR_PAYLOAD_PARTIAL_COUNTS = HDR_PAYLOAD_COUNTS / 2

def fill_counts(payload, last_index, start=0):
    # note that this function should only be used for
    # raw payload level operations, shoud not be used for payloads that are
    # created from a histogram, see fill_hist_counts
    counts = payload.get_counts()
    for index in xrange(start, last_index):
        counts[index] = index

def check_counts(payload, last_index, multiplier=1, start=0):
    counts = payload.get_counts()
    for index in xrange(start, last_index):
        assert(counts[index] == multiplier * index)

def check_hdr_payload(counter_size):
    # Create an HdrPayload class with given counters count
    payload = HdrPayload(counter_size, HDR_PAYLOAD_COUNTS)
    # put some known numbers in the buckets
    fill_counts(payload, HDR_PAYLOAD_COUNTS)

    # get a compressed version of that payload
    cpayload = payload.compress()
    # now decompress it into a new hdr payload instance
    dpayload = HdrPayload().decompress(cpayload)

    assert(dpayload.counts_len == HDR_PAYLOAD_COUNTS)

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
        HdrPayload().decompress("junk data")

    # unsupported word size
    with pytest.raises(ValueError):
        payload = HdrPayload(1, HDR_PAYLOAD_COUNTS)
    with pytest.raises(ValueError):
        payload = HdrPayload(1000, HDR_PAYLOAD_COUNTS)

    # invalid cookie
    payload = HdrPayload(8, HDR_PAYLOAD_COUNTS)
    payload.payload['cookie'] = 12345
    cpayload = payload.compress()
    with pytest.raises(HdrCookieException):
        HdrPayload().decompress(cpayload)

def fill_hist_counts(histogram, last_index, start=0):
    # fill the counts of a given histogram and update the min/max/total count
    # accordingly
    for index in xrange(start, last_index):
        value_from_index = histogram.get_value_from_index(index)
        histogram.record_value(value_from_index, index)

def check_hist_counts(histogram, last_index, multiplier=1, start=0):
    for index in xrange(start, last_index):
        assert(histogram.get_count_at_index(index) == multiplier * index)

# This is the max latency used by wrk2
WRK2_MAX_LATENCY = 24 * 60 * 60 * 1000000

def check_hist_encode(word_size,
                      digits,
                      b64_wrap,
                      expected_compressed_length,
                      fill_start_percent,
                      fill_count_percent):
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, digits,
                             b64_wrap=b64_wrap,
                             word_size=word_size)
    if fill_count_percent:
        fill_start_index = (fill_start_percent * histogram.counts_len) / 100
        fill_to_index = fill_start_index + (fill_count_percent * histogram.counts_len) / 100
        fill_hist_counts(histogram, fill_to_index, fill_start_index)
    b64 = histogram.encode()
    assert(len(b64) == expected_compressed_length)

# A list of call arguments to check_hdr_encode
ENCODE_ARG_LIST = (
    # word size digits  b64_wrap expected_compressed_length, fill_start%, fill_count%
    # best case when all counters are zero
    (8, 3, True, 52, 0, 0),        # 385 = size when compressing entire counts array
    (8, 3, False, 30, 0, 0),       # 276   (instead of truncating trailing zero buckets)
    (8, 2, True, 52, 0, 0),        # 126
    (8, 2, False, 30, 0, 0),       # 85
    # typical case when all counters are aggregated in a small contiguous area
    (8, 3, True, 16452, 30, 20),   # 17172
    (8, 3, False, 12330, 30, 20),  # 12712
    (8, 2, True, 2096, 30, 20),    # 2212
    (8, 2, False, 1563, 30, 20),   # 1630
    # worst case when all counters are different
    (8, 3, True, 80680, 0, 100),
    (8, 3, False, 60501, 0, 100),
    (8, 2, True, 10744, 0, 100),
    (8, 2, False, 8048, 0, 100),
    # worst case 32-bit and 16-bit counters
    (4, 3, True, 76272, 0, 100),
    (4, 2, True, 10144, 0, 100),
    (2, 3, True, 68936, 0, 100),
    (2, 2, True, 9144, 0, 100),
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
    'HISTggAAAMx4nO3OMQ2AMBRF0V9qoALYGZFQbSQ4qAU0EWQggRC6oICEnDPct75xbUsM8xGP3Dfd2d'
    'sW9QwAAAAAAAAA4GUqXz8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH4nXZWnBjY=',
    # standard Hdr test corrected histogram
    'HISTggAABER4nO3ZwY0dRRRA0bGNYEsA7FkSArEhkQEpEBMiDEJACNcCSyPKZU/3repzFv6a8ftPt0'
    'qt77H9w6+//fLy/qc/X/714ePru39++eO3319+/usFAAAAAAAAAPiPH7+/uwAAAAAAAAAAAAAAAAAA'
    'AAAAAAAAAAAAAAAAAt7dHQAAAAAAAAAAfDH//w8AAAAAb8+/wwEAAG/N3zsAAPbi5zcAgD35OQ4A7u'
    'fPY+B0PueAU/l8A07l8w04lc834HQ+54BT+XxjJ55XruR54w6eO67keeNKnjeu5HnjSp43ruR540qe'
    'N67keeMOnjuu5HnjSp63e7j3Ne5tjXtb497WuLcv4/7WuLc17m2Ne1vj3ta4tzXubY17W+Pe1ri3Ne'
    '5tjXtb497WuLc17m2Ne/sy7m+Ne1vj3ta4tzWn31v1fNWuodpX7RqqfdWuodpX7RqqfdWuQd+aatdQ'
    '7at2DdW+atdQ7at2DdW+atdQ7at2DdW+atdQ7at2DfrWVLuGal+1a6j2VbuGal+1a6j2VbuGal+1a6'
    'j2VbsGfWuqXUO1r9o1VPuqXUO1r9o11Pq+ds/X2lftqu+rdj1tX7Wrvq/aVd9X7XravmpXfV+1q76v'
    '2lXfV+162r5qV31ftau+r9pV31ftetq+ald9X7Wrvq/aVd9X7XravmpXfV+1q76v2vW0fdWu+r5qV3'
    '1ftau+r9r1lH2z7796rtpVn6t2nTJX7arPVbtOmat2nTJX7arPVbtOmat21eeqXafMVbvqc9WuU+aq'
    'XfW5atcpc9Wu+ly165S5ald9rtp1yly1qz5X7Tplrtp1yly1qz5X7TplrtpVn6t2nTL3ub//2vzVc5'
    'WO2Tkda3OVjtm5SsfsnI61uUrH7FylY3au0jE7p2NtrtIxO1fpmJ3TsTZX6Zidq3TMzulYm6t0zM5V'
    'OmbndKzNVTpm5yods3OVjtk5HWtzlY7ZuUrH7JyOtblKx+xcpWN2TsfaXKVjdq7SMTtX6Zid07E2V+'
    'mYnat0zM7pWJurdMzOVTpm5/7vfa+9zs597ry9rQ577b1ib6XDXnuv2FvpsNfeYoe99l6xt9Jhr73F'
    'DnvtvWJvpcNee4sd9tp7xd5Kh732XrG30mGvvcUOe5+59/0rX7/19+096xy77T3lHLvtPeUcu+095R'
    'y77T3lHO7nrL2nnGO3vaecY7e9p5zD/Zy195Rz7Lb3lHPstveUc+y295RzuJ+z9p5yjt32nnKO3fae'
    'co7d9p5yDvdz1t5TzrHb3lPO8aXv//DJ6zef+f23ep+es/bquWevnr16nnLOWs9Tzqnnnr169up5yj'
    'lrPU85p5579uq5Z6+evXqecs5az1POqeeevXru2atnr56nnLPW85Rz6rlnr569ep5yzrt6vvv4+u0n'
    'X7/2/c/92p5r9tT77Nm7z569++zZu8+ea/bU++zZu8+evfvs2btv2z1/A96FLUU='
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

# Encode sample as generated by wrk2 running HdrHistogram_c 0.0.1
# These are 2 digit precision histograms
ENCODE_SAMPLE_WRK2_C = [
    "HISTggAAAL94nO3SywnCUBBA0clLYkS3LrUCi0grlqJgBxZnG5agYGZz5WGEgJt7QIb55P1wf71d"
    "Io7neCtTbF6/3eF+ivERkiRJkiRJkiRJkiRJkiT9S1Opt1PskWfcTLHDXF+pb1Ev6GdcY44x+wUx"
    "+y3qgXrGnG8QC+ZSh3xV2Y/n5f0S7zmgn/Pch+9XMMfv+G65zoA878/vAv0eOd+L6xXkrAdy1mv4"
    "v/2Ws17rz8Vzzt1/qf1+tfR5ll7vY/0nEdkGJg=="
]

@pytest.mark.codec
def test_hdr_interop_wrk2():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, 2)
    histogram.decode_and_add(ENCODE_SAMPLE_WRK2_C[0])


def check_cod_perf():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, 2)
    fill_start_index = (20 * histogram.counts_len) / 100
    fill_to_index = fill_start_index + (30 * histogram.counts_len) / 100
    fill_hist_counts(histogram, fill_to_index, fill_start_index)

    # encode 1000 times
    start = datetime.datetime.now()
    for _ in xrange(1000):
        histogram.encode()
    delta = datetime.datetime.now() - start
    print delta

def check_dec_perf():
    histogram = HdrHistogram(LOWEST, WRK2_MAX_LATENCY, 2)
    fill_start_index = (20 * histogram.counts_len) / 100
    fill_to_index = fill_start_index + (30 * histogram.counts_len) / 100
    fill_hist_counts(histogram, fill_to_index, fill_start_index)
    b64 = histogram.encode()

    # decode and add to self 1000 times
    start = datetime.datetime.now()
    for _ in xrange(1000):
        histogram.decode_and_add(b64)
    delta = datetime.datetime.now() - start
    print delta

@pytest.mark.perf
def test_cod_perf():
    cProfile.runctx('check_cod_perf()', globals(), locals())

@pytest.mark.perf
def test_dec_perf():
    cProfile.runctx('check_dec_perf()', globals(), locals())

def check_decoded_hist_counts(hist, multiplier):
    assert(hist)
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
    assert(log_reader.get_next_interval_histogram() is None)

JHICCUP_V1_LOG_NAME = "test/jHiccup-2.0.6.logV1.hlog"
# Test input and expected output values
JHICCUP_CHECKLISTS = [
    {'target': {'histogram_count': 88,
                'total_count': 65964,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1829765119,
                'accumulated_histogram.get_max_value()': 1888485375,
                'log_reader.get_start_time_sec()': 1438867590.285}},
    {'range_start_time_sec': 5,
     'range_end_time_sec': 20,
     'target': {'histogram_count': 15,
                'total_count': 11213,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1019740159,
                'accumulated_histogram.get_max_value()': 1032323071}},
    {'range_start_time_sec': 50,
     'range_end_time_sec': 80,
     'target': {'histogram_count': 29,
                'total_count': 22630,
                'accumulated_histogram.get_value_at_percentile(99.9)': 1871708159,
                'accumulated_histogram.get_max_value()': 1888485375}}
]

@pytest.mark.log
def test_jHiccup_v1_log():
    accumulated_histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    for checklist in JHICCUP_CHECKLISTS:
        accumulated_histogram.reset()
        log_reader = HistogramLogReader(JHICCUP_V1_LOG_NAME, accumulated_histogram)

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
            # These logs use 2-word (16-bit) counters
            assert(decoded_histogram.get_word_size() == 2)
        for statement in target_numbers:
            assert(eval(statement) == target_numbers[statement])

        log_reader.close()
