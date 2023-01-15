'''
A pure python version of the hdr_histogram code

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
from __future__ import division, print_function
import math
import sys
from hdrh.iterators import AllValuesIterator
from hdrh.iterators import RecordedIterator
from hdrh.iterators import PercentileIterator
from hdrh.iterators import LinearIterator
from hdrh.iterators import LogIterator
from hdrh.codec import HdrHistogramEncoder

def get_bucket_count(value, subb_count, unit_mag):
    smallest_untrackable_value = subb_count << unit_mag
    buckets_needed = 1
    while smallest_untrackable_value <= value:
        if smallest_untrackable_value > sys.maxsize // 2:
            return buckets_needed + 1
        smallest_untrackable_value <<= 1
        buckets_needed += 1
    return buckets_needed

class HdrHistogram():
    '''This class supports the recording and analyzing of sampled data value
    counts across a configurable integer value range with configurable value
    precision within the range. Value precision is expressed as the number of
    significant digits in the value recording, and provides control over value
    quantization behavior across the value range and the subsequent value
    resolution at any given level.

    For example, a Histogram could be configured to track the counts of
    observed integer values between 0 and 3,600,000,000 while maintaining a
    value precision of 3 significant digits across that range. Value
    quantization within the range will thus be no larger than 1/1,000th
    (or 0.1%) of any value. This example Histogram could be used to track and
    analyze the counts of observed response times ranging between 1 microsecond
    and 1 hour in magnitude, while maintaining a value resolution of 1
    microsecond up to 1 millisecond, a resolution of 1 millisecond (or better)
    up to one second, and a resolution of 1 second (or better) up to 1,000
    seconds. At it's maximum tracked value (1 hour), it would still maintain a
    resolution of 3.6 seconds (or better).
    '''

    def __init__(self,
                 lowest_trackable_value,
                 highest_trackable_value,
                 significant_figures,
                 word_size=8,
                 b64_wrap=True,
                 hdr_payload=None):
        '''Create a new histogram with given arguments

        Params:
            lowest_trackable_value The lowest value that can be discerned
                (distinguished from 0) by the histogram.
                Must be a positive integer that is >= 1.
                May be internally rounded down to nearest power of 2.
            highest_trackable_value The highest value to be tracked by the
                histogram. Must be a positive integer that is >=
                (2 * lowest_trackable_value).
            significant_figures The number of significant decimal digits to
                which the histogram will maintain value resolution and
                separation. Must be a non-negative integer between 0 and 5.
            word_size size of counters in bytes, only 2, 4, 8-byte counters
                are supported (default is 8-byte or 64-bit counters)
            b64_wrap specifies if the encoding of this histogram should use
                base64 wrapping (only useful if you need to encode the histogram
                to save somewhere or send over the wire. By default base64
                encoding is assumed
            hdr_payload only used for associating an existing payload created
                from decoding an encoded histograme
        Exceptions:
            ValueError if the word_size value is unsupported
                if significant_figures is invalid
        '''
        if significant_figures < 1 or significant_figures > 5:
            raise ValueError('Invalid significant_figures')
        self.lowest_trackable_value = lowest_trackable_value
        self.highest_trackable_value = highest_trackable_value
        self.significant_figures = significant_figures
        self.unit_magnitude = int(math.floor(math.log(lowest_trackable_value) / math.log(2)))
        largest_value_single_unit_res = 2 * math.pow(10, significant_figures)
        subb_count_mag = int(math.ceil(math.log(largest_value_single_unit_res) / math.log(2)))
        self.sub_bucket_half_count_magnitude = subb_count_mag - 1 if subb_count_mag > 1 else 0
        self.sub_bucket_count = int(math.pow(2, self.sub_bucket_half_count_magnitude + 1))
        self.sub_bucket_half_count = self.sub_bucket_count // 2
        self.sub_bucket_mask = (self.sub_bucket_count - 1) << self.unit_magnitude
        self.bucket_count = get_bucket_count(highest_trackable_value,
                                             self.sub_bucket_count,
                                             self.unit_magnitude)
        self.min_value = sys.maxsize
        self.max_value = 0
        self.total_count = 0
        self.counts_len = (self.bucket_count + 1) * (self.sub_bucket_count // 2)
        self.word_size = word_size

        if hdr_payload:
            payload = hdr_payload.payload
            self.int_to_double_conversion_ratio = payload.conversion_ratio_bits
            results = hdr_payload.init_counts(self.counts_len)
            if results['total']:
                self.set_internal_tacking_values(results['min_nonzero_index'],
                                                 results['max_nonzero_index'],
                                                 results['total'])
        else:
            self.int_to_double_conversion_ratio = 1.0

        # to encode this histogram into a compressed/base64 format ready
        # to be exported
        self.b64_wrap = b64_wrap
        self.encoder = HdrHistogramEncoder(self, b64_wrap, hdr_payload)
        # the counters reside directly in the payload object
        # allocated by the encoder
        # so that compression for wire transfer can be done without copy
        self.counts = self.encoder.get_counts()
        self.start_time_stamp_msec = 0
        self.end_time_stamp_msec = 0
        # no tag by default
        self.tag = None

    def _clz(self, value):
        """calculate the leading zeros, equivalent to C __builtin_clzll()
        value in hex:
        value = 1 clz = 63
        value = 2 clz = 62
        value = 4 clz = 61
        value = 1000 clz = 51
        value = 1000000 clz = 39
        """
        return 63 - (len(bin(value)) - 3)

    def _get_bucket_index(self, value):
        # smallest power of 2 containing value
        pow2ceiling = 64 - self._clz(int(value) | self.sub_bucket_mask)
        return int(pow2ceiling - self.unit_magnitude -
                   (self.sub_bucket_half_count_magnitude + 1))

    def _get_sub_bucket_index(self, value, bucket_index):
        return int(value) >> (bucket_index + self.unit_magnitude)

    def _counts_index(self, bucket_index, sub_bucket_index):
        # Calculate the index for the first entry in the bucket:
        # (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
        bucket_base_index = (bucket_index + 1) << self.sub_bucket_half_count_magnitude
        # Calculate the offset in the bucket:
        offset_in_bucket = sub_bucket_index - self.sub_bucket_half_count
        # The following is the equivalent of
        # ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex
        return bucket_base_index + offset_in_bucket

    def _counts_index_for(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)
        return self._counts_index(bucket_index, sub_bucket_index)

    def record_value(self, value, count=1):
        '''Record a new value into the histogram

        Args:
            value: the value to record (must be in the valid range)
            count: incremental count (defaults to 1)
        '''
        if value < 0:
            return False
        counts_index = self._counts_index_for(value)
        if (counts_index < 0) or (self.counts_len <= counts_index):
            return False
        self.counts[counts_index] += count
        self.total_count += count
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        return True

    # pylint: disable=inconsistent-return-statements
    def record_corrected_value(self, value, expected_interval, count=1):
        '''Record a new value into the histogram and correct for
        coordinated omission if needed

        Args:
            value: the value to record (must be in the valid range)
            expected_interval: the expected interval between 2 value samples
            count: incremental count (defaults to 1)
        '''
        while True:
            if not self.record_value(value, count):
                return False
            if value <= expected_interval or expected_interval <= 0:
                return True
            value -= expected_interval

    def get_count_at_index(self, index):
        if index >= self.counts_len:
            raise IndexError()
        # some decoded (read-only) histograms may have truncated
        # counts arrays, we return zero for any index that is passed the array
        if index >= self.encoder.payload.counts_len:
            return 0
        return self.counts[index]

    def get_count_at_sub_bucket(self, bucket_index, sub_bucket_index):
        # Calculate the index for the first entry in the bucket:
        # (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) )
        bucket_base_index = (bucket_index + 1) << self.sub_bucket_half_count_magnitude
        # Calculate the offset in the bucket:
        offset_in_bucket = sub_bucket_index - self.sub_bucket_half_count
        # The following is the equivalent of
        # (sub_bucket_index - subBucketHalfCount) + bucketBaseIndex
        counts_index = bucket_base_index + offset_in_bucket
        return self.counts[counts_index]

    def get_value_from_sub_bucket(self, bucket_index, sub_bucket_index):
        return sub_bucket_index << (bucket_index + self.unit_magnitude)

    def get_value_from_index(self, index):
        bucket_index = (index >> self.sub_bucket_half_count_magnitude) - 1
        sub_bucket_index = (index & (self.sub_bucket_half_count - 1)) + \
            self.sub_bucket_half_count
        if bucket_index < 0:
            sub_bucket_index -= self.sub_bucket_half_count
            bucket_index = 0
        return self.get_value_from_sub_bucket(bucket_index, sub_bucket_index)

    def get_lowest_equivalent_value(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)

        lowest_equivalent_value = self.get_value_from_sub_bucket(bucket_index,
                                                                 sub_bucket_index)
        return lowest_equivalent_value

    def get_highest_equivalent_value(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)

        lowest_equivalent_value = self.get_value_from_sub_bucket(bucket_index,
                                                                 sub_bucket_index)
        if sub_bucket_index >= self.sub_bucket_count:
            bucket_index += 1
        size_of_equivalent_value_range = 1 << (self.unit_magnitude + bucket_index)
        next_non_equivalent_value = lowest_equivalent_value + size_of_equivalent_value_range

        return next_non_equivalent_value - 1

    def get_target_count_at_percentile(self, percentile):
        requested_percentile = min(percentile, 100.0)
        count_at_percentile = int(((requested_percentile * self.total_count / 100)) + 0.5)
        return max(count_at_percentile, 1)

    def get_value_at_percentile(self, percentile):
        '''Get the value for a given percentile

        Args:
            percentile: a float in [0.0..100.0]
        Returns:
            the value for the given percentile
        '''
        count_at_percentile = self.get_target_count_at_percentile(percentile)
        total = 0
        for index in range(self.counts_len):
            total += self.get_count_at_index(index)
            if total >= count_at_percentile:
                value_at_index = self.get_value_from_index(index)
                if percentile:
                    return self.get_highest_equivalent_value(value_at_index)
                return self.get_lowest_equivalent_value(value_at_index)
        return 0

    def get_percentile_to_value_dict(self, percentile_list):
        '''A faster alternative to query values for a list of percentiles.

        Args:
            percentile_list: a list of percentiles in any order, dups will be ignored
            each element in the list must be a float value in [0.0 .. 100.0]
        Returns:
            a dict of percentile values indexed by the percentile
        '''
        result = {}
        total = 0
        percentile_list_index = 0
        count_at_percentile = 0
        # remove dups and sort
        percentile_list = list(set(percentile_list))
        percentile_list.sort()

        for index in range(self.counts_len):
            total += self.get_count_at_index(index)
            while True:
                # recalculate target based on next requested percentile
                if not count_at_percentile:
                    if percentile_list_index == len(percentile_list):
                        return result
                    percentile = percentile_list[percentile_list_index]
                    percentile_list_index += 1
                    if percentile > 100:
                        return result
                    count_at_percentile = self.get_target_count_at_percentile(percentile)

                if total >= count_at_percentile:
                    value_at_index = self.get_value_from_index(index)
                    if percentile:
                        result[percentile] = self.get_highest_equivalent_value(value_at_index)
                    else:
                        result[percentile] = self.get_lowest_equivalent_value(value_at_index)
                    count_at_percentile = 0
                else:
                    break
        return result

    def get_total_count(self):
        return self.total_count

    def get_count_at_value(self, value):
        counts_index = self._counts_index_for(value)
        return self.counts[counts_index]

    def values_are_equivalent(self, val1, val2):
        '''Check whether 2 values are equivalent (meaning they
        are in the same bucket/range)

        Returns:
            true if the 2 values are equivalent
        '''
        return self.get_lowest_equivalent_value(val1) == self.get_lowest_equivalent_value(val2)

    def get_max_value(self):
        if self.max_value == 0:
            return 0
        return self.get_highest_equivalent_value(self.max_value)

    def get_min_value(self):
        if self.counts[0] > 0 or self.total_count == 0:
            return 0
        if sys.maxsize == self.min_value:
            return sys.maxsize
        return self.get_lowest_equivalent_value(self.min_value)

    def _hdr_size_of_equiv_value_range(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)
        if sub_bucket_index >= self.sub_bucket_count:
            bucket_index += 1
        return 1 << (self.unit_magnitude + bucket_index)

    def _hdr_median_equiv_value(self, value):
        return self.get_lowest_equivalent_value(value) + \
            (self._hdr_size_of_equiv_value_range(value) >> 1)

    def get_mean_value(self):
        if not self.total_count:
            return 0.0
        total = 0
        itr = self.get_recorded_iterator()
        for item in itr:
            total += itr.count_at_this_value * self._hdr_median_equiv_value(item.value_iterated_to)
        return float(total) / self.total_count

    def get_stddev(self):
        if not self.total_count:
            return 0.0
        mean = self.get_mean_value()
        geometric_dev_total = 0.0
        for item in self.get_recorded_iterator():
            dev = (self._hdr_median_equiv_value(item.value_iterated_to) * 1.0) - mean
            geometric_dev_total += (dev * dev) * item.count_added_in_this_iter_step
        return math.sqrt(geometric_dev_total / self.total_count)

    def reset(self):
        '''Reset the histogram to a pristine state
        '''
        for index in range(self.counts_len):
            self.counts[index] = 0
        self.total_count = 0
        self.min_value = sys.maxsize
        self.max_value = 0
        self.start_time_stamp_msec = sys.maxsize
        self.end_time_stamp_msec = 0

    def __iter__(self):
        '''Returns the recorded iterator if iter(self) is called
        '''
        return RecordedIterator(self)

    def get_all_values_iterator(self):
        return AllValuesIterator(self)

    def get_recorded_iterator(self):
        return RecordedIterator(self)

    def get_percentile_iterator(self, ticks_per_half_distance):
        return PercentileIterator(self, ticks_per_half_distance)

    def get_linear_iterator(self, value_units_per_bucket):
        return LinearIterator(self, value_units_per_bucket)

    def get_log_iterator(self, value_units_first_bucket, log_base):
        return LogIterator(self, value_units_first_bucket, log_base)

    def encode(self):
        '''
        Encode this histogram into a histoblob.

        Return:
            the histoblob describing this histogram (a string containing the base64 encoded
            compressed histogram, V2 format)
        '''
        return self.encoder.encode()

    # def adjust_internal_tacking_values(self,
    #                                   min_non_zero_index,
    #                                   max_index,
    #                                   total_added):
    #    '''Called during decoding and add to adjust the new min/max value and
    #    total count

    #   Args:
    #        min_non_zero_index min nonzero index of all added counts (-1 if none)
    #        max_index max index of all added counts (-1 if none)
    #    '''
    #    if max_index >= 0:
    #        max_value = self.get_highest_equivalent_value(self.get_value_from_index(max_index))
    #        self.max_value = max(self.max_value, max_value)
    #    if min_non_zero_index >= 0:
    #        min_value = self.get_value_from_index(min_non_zero_index)
    #        self.min_value = min(self.min_value, min_value)
    #    self.total_count += total_added

    def set_internal_tacking_values(self,
                                    min_non_zero_index,
                                    max_index,
                                    total_added):
        '''Called during decoding and add to adjust the new min/max value and
        total count

        Args:
            min_non_zero_index min nonzero index of all added counts (-1 if none)
            max_index max index of all added counts (-1 if none)
        '''
        if max_index >= 0:
            self.max_value = self.get_highest_equivalent_value(self.get_value_from_index(max_index))
        if min_non_zero_index >= 0:
            self.min_value = self.get_value_from_index(min_non_zero_index)
        self.total_count = total_added

    def get_counts_array_index(self, value):
        '''Return the index in the counts array for a given value
        '''
        if value < 0:
            raise ValueError("Histogram recorded value cannot be negative.")

        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)
        # Calculate the index for the first entry in the bucket:
        bucket_base_index = (bucket_index + 1) << self.sub_bucket_half_count_magnitude
        # The following is the equivalent of ((bucket_index + 1) * sub_bucket_half_count)
        # Calculate the offset in the bucket (can be negative for first bucket):
        offset_in_bucket = sub_bucket_index - self.sub_bucket_half_count
        # The following is the equivalent of
        # ((sub_bucket_index  - sub_bucket_half_count) + bucket_base_index
        return bucket_base_index + offset_in_bucket

    def get_start_time_stamp(self):
        return self.start_time_stamp_msec

    def set_start_time_stamp(self, time_stamp_msec):
        '''Set the start time stamp value associated with this histogram to a given value.
        Params:
            time_stamp_msec the value to set the time stamp to,
                [by convention] in msec since the epoch.
        '''
        self.start_time_stamp_msec = time_stamp_msec

    def get_end_time_stamp(self):
        return self.end_time_stamp_msec

    def set_end_time_stamp(self, time_stamp_msec):
        '''Set the end time stamp value associated with this histogram to a given value.
        Params:
            time_stamp_msec the value to set the time stamp to,
                [by convention] in msec since the epoch.
        '''
        self.end_time_stamp_msec = time_stamp_msec

    def set_tag(self, tag):
        """
        Associate a tag to this histogram.
        """
        self.tag = tag

    def get_tag(self):
        """
        Retrieve the tag for this histogram.
        """
        return self.tag

    def add(self, other_hist):
        highest_recordable_value = \
            self.get_highest_equivalent_value(self.get_value_from_index(self.counts_len - 1))
        if highest_recordable_value < other_hist.get_max_value():
            raise IndexError("The other histogram includes values that do not fit %d < %d" %
                             (highest_recordable_value, other_hist.get_max_value()))

        if (self.bucket_count == other_hist.bucket_count) and \
           (self.sub_bucket_count == other_hist.sub_bucket_count) and \
           (self.unit_magnitude == other_hist.unit_magnitude) and \
           (self.word_size == other_hist.word_size):

            # do an in-place addition of one array to another
            self.encoder.add(other_hist.encoder)

            self.total_count += other_hist.get_total_count()
            self.max_value = max(self.max_value, other_hist.get_max_value())
            self.min_value = min(self.get_min_value(), other_hist.get_min_value())
        else:
            # Arrays are not a direct match, so we can't just stream through and add them.
            # Instead, go through the array and add each non-zero value found at it's proper value:
            for index in range(other_hist.counts_len):
                other_count = other_hist.get_count_at_index(index)
                if other_count > 0:
                    self.record_value(other_hist.get_value_from_index(index), other_count)

        self.start_time_stamp_msec = \
            min(self.start_time_stamp_msec, other_hist.start_time_stamp_msec)
        self.end_time_stamp_msec = \
            max(self.end_time_stamp_msec, other_hist.end_time_stamp_msec)

    def decode_and_add(self, encoded_histogram):
        '''Decode an encoded histogram and add it to this histogram
        Args:
            encoded_histogram (string) a histoblob encoded histogram
                following the V2 format, such as one returned by the encode() method
        Exception:
            TypeError in case of base64 decode error
            HdrCookieException:
                the main header has an invalid cookie
                the compressed payload header has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            zlib.error:
                in case of zlib decompression error
        '''
        other_hist = HdrHistogram.decode(encoded_histogram, self.b64_wrap)
        self.add(other_hist)

    @staticmethod
    def decode(encoded_histogram, b64_wrap=True):
        '''Decode a histoblob and return a new histogram instance that
        has been initialized with the decoded content
        Return:
            a new histogram instance representing the decoded content
        Exception:
            TypeError in case of base64 decode error
            HdrCookieException:
                the main header has an invalid cookie
                the compressed payload header has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            zlib.error:
                in case of zlib decompression error
        '''
        hdr_payload = HdrHistogramEncoder.decode(encoded_histogram, b64_wrap)
        payload = hdr_payload.payload
        histogram = HdrHistogram(payload.lowest_trackable_value,
                                 payload.highest_trackable_value,
                                 payload.significant_figures,
                                 hdr_payload=hdr_payload)
        return histogram

    def get_word_size(self):
        return self.word_size

    def get_int_to_double_conversion_ratio(self):
        return self.int_to_double_conversion_ratio

    def output_percentile_distribution(self,
                                       out_file,
                                       output_value_unit_scaling_ratio,
                                       ticks_per_half_distance=5,
                                       use_csv=False):
        if use_csv:
            out_file.write(b'"Value","Percentile","TotalCount","1/(1-Percentile)"\n')
        else:
            out_file.write(b'%12s %14s %10s %14s\n\n' %
                           (b'Value', b'Percentile', b'TotalCount', b'1/(1-Percentile)'))

        if use_csv:
            percentile_format = '%.{}f,%.12f,%d,%.2f\n'.format(self.significant_figures)
            last_line_percentile_format = '%.{}f,%.12f,%d,Infinity\n'.format(self.significant_figures)
        else:
            percentile_format = '%12.{}f %2.12f %10d %14.2f\n'.format(self.significant_figures)
            last_line_percentile_format = '%12.{}f %2.12f %10d\n'.format(self.significant_figures)

        for iter_value in self.get_percentile_iterator(ticks_per_half_distance):
            value = iter_value.value_iterated_to / output_value_unit_scaling_ratio
            percentile = iter_value.percentile_level_iterated_to / 100
            total_count = iter_value.total_count_to_this_value
            if iter_value.percentile_level_iterated_to != 100:
                other = 1 / (1 - iter_value.percentile_level_iterated_to / 100)
                out_file.write(percentile_format.encode() % (value, percentile,
                                                             total_count, other))
            else:
                out_file.write(last_line_percentile_format.encode() % (value,
                                                                       percentile,
                                                                       total_count))

        if use_csv:
            return

        mean = self.get_mean_value() / output_value_unit_scaling_ratio
        stddev = self.get_stddev()
        out_file.write('#[Mean    = %12.{0}f, StdDeviation   = %12.{0}f]\n'.
                       format(self.significant_figures).encode() % (mean, stddev))

        max_value = self.get_max_value() / output_value_unit_scaling_ratio
        total = self.get_total_count()
        out_file.write('#[Max     = %12.{0}f, TotalCount     = %12.{0}f]\n'.format(
            self.significant_figures).encode() % (max_value, total))
        out_file.write(b'#[Buckets = %12d, SubBuckets     = %12d]\n' % (
            self.bucket_count, self.sub_bucket_count))

    @staticmethod
    def dump(encoded_histogram, output=None,
             output_value_unit_scaling_ratio=1):
        """
        Dump a the histogram percentile table (.hgrm) to the provider output.

        param output: a writable buffer output,
                      if None output will be written to stdout
        param output_value_unit_scaling_ratio: scaling ratio, the amount
                    by which values will be divided for display, defaults to 1
        """
        histogram = HdrHistogram.decode(encoded_histogram)

        if output is None:
            # sys.stdout.buffer will raise AttributeError in python 2.7
            try:
                # python 3 requires .buffer to write bytes
                output = sys.stdout.buffer
            except AttributeError:
                # in python 2.7, bytes can be writtent to sys.stdout
                output = sys.stdout
        histogram.output_percentile_distribution(output,
                                                 output_value_unit_scaling_ratio)

    def equals(self, other):
        """
        Check if this instance of histogram is equal to the provided histogram.

        other: the other histogram to compare to
        return: True if this histogram are equivalent with the other.
        """
        if self == other:
            return True
        if self.lowest_trackable_value != other.lowest_trackable_value or \
           self.int_to_double_conversion_ratio != other.int_to_double_conversion_ratio or \
           self.significant_figures != other.significant_figures:
            return False
        if self.get_total_count() != other.get_total_count():
            return False
        if self.get_max_value() != other.get_max_value():
            return False
        #         if (getMinNonZeroValue() != that.getMinNonZeroValue()) {
        if self.get_min_value() != other.get_min_value():
            return False
        if self.counts_len != other.counts_len:
            return False
        for index in range(self.counts_len):
            if self.get_count_at_index(index) != other.get_count_at_index(index):
                return False
        return True
