'''
A pure python version of the hdr_histogram code

Ported from
https://github.com/HdrHistogram/HdrHistogram (Java)
https://github.com/HdrHistogram/HdrHistogram_c (C)

Histogram Iterators: all values, recorded, linear and logarithmic

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
from builtins import object
from abc import abstractmethod
import math

class HdrConcurrentModificationException(Exception):
    pass

class HdrIterationValue(object):
    '''Class of the values returned by each iterator
    '''
    def __init__(self, hdr_iterator):
        self.hdr_iterator = hdr_iterator
        self.value_iterated_to = 0
        self.value_iterated_from = 0,
        self.count_at_value_iterated_to = 0
        self.count_added_in_this_iter_step = 0
        self.total_count_to_this_value = 0
        self.total_value_to_this_value = 0
        self.percentile = 0.0
        self.percentile_level_iterated_to = 0.0
        self.int_to_double_conversion_ratio = 0.0

    def set(self, value_iterated_to):
        hdr_it = self.hdr_iterator
        self.value_iterated_to = value_iterated_to
        self.value_iterated_from = hdr_it.prev_value_iterated_to
        self.count_at_value_iterated_to = hdr_it.count_at_this_value
        self.count_added_in_this_iter_step = \
            hdr_it.total_count_to_current_index - hdr_it.total_count_to_prev_index
        self.total_count_to_this_value = hdr_it.total_count_to_current_index
        self.total_value_to_this_value = hdr_it.value_to_index
        self.percentile = (100.0 * hdr_it.total_count_to_current_index) / hdr_it.total_count
        self.percentile_level_iterated_to = hdr_it.get_percentile_iterated_to()
        self.int_to_double_conversion_ratio = hdr_it.int_to_double_conversion_ratio

class AbstractHdrIterator(object):
    '''Provide a means of iterating through all histogram values using the finest
    granularity steps supported by the underlying representation.
    The iteration steps through all possible unit value levels, regardless of
    whether or not there were recorded values for that value level,
    and terminates when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram):
        self.histogram = histogram
        self.current_index = 0
        self.count_at_this_value = 0
        self.total_count_to_current_index = 0
        self.total_count_to_prev_index = 0
        self.prev_value_iterated_to = 0
        self.value_at_index = 0
        self.value_to_index = 0
        self.value_at_next_index = 1 << histogram.unit_magnitude
        self.current_iteration_value = HdrIterationValue(self)
        # take a snapshot of the total count
        self.total_count = histogram.total_count
        self.int_to_double_conversion_ratio = histogram.int_to_double_conversion_ratio
        self.fresh_sub_bucket = True

    def __iter__(self):
        self.reset_iterator(self.histogram)
        return self

    def reset_iterator(self, histogram):
        if not histogram:
            histogram = self.histogram
        self.histogram = histogram
        self.current_index = 0
        self.count_at_this_value = 0
        self.total_count_to_current_index = 0
        self.total_count_to_prev_index = 0
        self.prev_value_iterated_to = 0
        self.value_at_index = 0
        self.value_to_index = 0
        self.value_at_next_index = 1 << histogram.unit_magnitude
        self.current_iteration_value = HdrIterationValue(self)
        # take a snapshot of the total count
        self.total_count = histogram.total_count
        self.int_to_double_conversion_ratio = histogram.int_to_double_conversion_ratio
        self.fresh_sub_bucket = True

    def has_next(self):
        return self.total_count_to_current_index < self.total_count

    def __next__(self):
        if self.total_count != self.histogram.total_count:
            raise HdrConcurrentModificationException()
        while self.has_next():
            self.count_at_this_value = self.histogram.get_count_at_index(self.current_index)
            if self.fresh_sub_bucket:
                self.total_count_to_current_index += self.count_at_this_value
                self.value_to_index += self.count_at_this_value * self.get_value_iterated_to()
                self.fresh_sub_bucket = False
            if self.reached_iteration_level():
                value_iterated_to = self.get_value_iterated_to()
                self.current_iteration_value.set(value_iterated_to)

                self.prev_value_iterated_to = value_iterated_to
                self.total_count_to_prev_index = self.total_count_to_current_index

                self.increment_iteration_level()

                if self.total_count != self.histogram.total_count:
                    raise HdrConcurrentModificationException()

                return self.current_iteration_value

            # get to the next sub bucket
            self.increment_sub_bucket()

        if self.total_count_to_current_index > self.total_count_to_prev_index:
            # We are at the end of the iteration but we still need to report
            # the last iteration value
            value_iterated_to = self.get_value_iterated_to()
            self.current_iteration_value.set(value_iterated_to)
            # we do this one time only
            self.total_count_to_prev_index = self.total_count_to_current_index
            return self.current_iteration_value

        raise StopIteration()

    @abstractmethod
    def reached_iteration_level(self):
        pass

    @abstractmethod
    def increment_iteration_level(self):
        pass

    def increment_sub_bucket(self):
        self.fresh_sub_bucket = True
        self.current_index += 1
        self.value_at_index = self.histogram.get_value_from_index(self.current_index)
        self.value_at_next_index = \
            self.histogram.get_value_from_index(self.current_index + 1)

    def get_value_iterated_to(self):
        return self.histogram.get_highest_equivalent_value(self.value_at_index)

    def get_percentile_iterated_to(self):
        return (100.0 * self.total_count_to_current_index) / self.total_count

    def get_percentile_iterated_from(self):
        return (100.0 * self.total_count_to_prev_index) / self.total_count

class AllValuesIterator(AbstractHdrIterator):
    def __init__(self, histogram):
        AbstractHdrIterator.__init__(self, histogram)
        self.visited_index = -1

    def reset(self, histogram=None):
        AbstractHdrIterator.reset_iterator(self, histogram)
        self.visited_index = -1

    def reached_iteration_level(self):
        return self.visited_index != self.current_index

    def increment_iteration_level(self):
        self.visited_index = self.current_index

    def has_next(self):
        if self.total_count != self.histogram.total_count:
            raise HdrConcurrentModificationException()
        return self.current_index < self.histogram.counts_len - 1

class RecordedIterator(AllValuesIterator):
    '''Provide a means of iterating through all recorded histogram values
    using the finest granularity steps supported by the underlying representation.
    The iteration steps through all non-zero recorded value counts,
    and terminates when all recorded histogram values are exhausted.
    '''
    def reached_iteration_level(self):
        current_count = self.histogram.get_count_at_index(self.current_index)
        return current_count and self.visited_index != self.current_index

class AbstractLiLoIteratortype(AbstractHdrIterator):
    '''Linear/Log iterator common parent class
    '''
    def __init__(self, histogram, next_value_report_lev):
        AbstractHdrIterator.__init__(self, histogram)
        self.next_value_report_lev = next_value_report_lev
        self.next_value_report_lev_lowest_eq = \
            histogram.get_lowest_equivalent_value(next_value_report_lev)

    def reset(self, histogram, next_value_report_lev):
        AbstractHdrIterator.reset_iterator(self, histogram)
        if next_value_report_lev:
            self.next_value_report_lev = next_value_report_lev
        self.next_value_report_lev_lowest_eq = \
            self.histogram.get_lowest_equivalent_value(next_value_report_lev)

    def has_next(self):
        if AbstractHdrIterator.has_next(self):
            return True
        # If next iterate does not move to the next sub bucket index (which is empty if
        # if we reached this point), then we are not done iterating... Otherwise we're done.
        return self.next_value_report_lev_lowest_eq < self.value_at_next_index

    def reached_iteration_level(self):
        return self.value_at_index >= self.next_value_report_lev_lowest_eq

    def get_value_iterated_to(self):
        return self.next_value_report_lev

class LinearIterator(AbstractLiLoIteratortype):
    '''Provide a means of iterating through histogram values using linear steps.

    The iteration is performed in steps of value_units_per_bucket in size,
    terminating when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, value_units_per_bucket):
        AbstractLiLoIteratortype.__init__(self, histogram, value_units_per_bucket)
        self.value_units_per_bucket = value_units_per_bucket

    def reset(self, histogram=None, value_units_per_bucket=0):
        AbstractLiLoIteratortype.reset(self, histogram, value_units_per_bucket)
        if value_units_per_bucket:
            self.value_units_per_bucket = value_units_per_bucket

    def increment_iteration_level(self):
        self.next_value_report_lev += self.value_units_per_bucket
        self.next_value_report_lev_lowest_eq = \
            self.histogram.get_lowest_equivalent_value(self.next_value_report_lev)

class LogIterator(AbstractLiLoIteratortype):
    '''Provide a means of iterating through histogram values at logarithmically
    increasing levels.

    The iteration is performed in steps that start at value_units_first_bucket
    and increase exponentially according to log_base, terminating when all
    recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, value_units_first_bucket, log_base):
        AbstractLiLoIteratortype.__init__(self, histogram, value_units_first_bucket)
        self.log_base = log_base

    def reset(self, histogram=None, value_units_first_bucket=0, log_base=0):
        AbstractLiLoIteratortype.reset(self, histogram, value_units_first_bucket)
        if log_base:
            self.log_base = log_base

    def increment_iteration_level(self):
        self.next_value_report_lev *= self.log_base
        self.next_value_report_lev_lowest_eq = \
            self.histogram.get_lowest_equivalent_value(self.next_value_report_lev)

class PercentileIterator(AbstractHdrIterator):
    '''Provide a means of iterating through histogram values according to
    percentile levels

    The iteration is performed in steps that start at 0% and reduce their
    distance to 100% according to the ticks_per_half_distance parameter,
    ultimately reaching 100% when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, percentile_ticks_per_half_distance):
        AbstractHdrIterator.__init__(self, histogram)
        self.percentile_ticks_per_half_distance = percentile_ticks_per_half_distance
        self.percentile_to_iterate_to = 0
        self.percentile_to_iterate_from = 0
        self.reached_last_recorded_value = False

    def reset(self, histogram=None, percentile_ticks_per_half_distance=0):
        AbstractHdrIterator.reset_iterator(self, histogram)
        if percentile_ticks_per_half_distance:
            self.percentile_ticks_per_half_distance = percentile_ticks_per_half_distance
        self.percentile_to_iterate_to = 0
        self.percentile_to_iterate_from = 0
        self.reached_last_recorded_value = False

    def has_next(self):
        if AbstractHdrIterator.has_next(self):
            return True

        # We want one additional last step to 100%
        if not self.reached_last_recorded_value and self.total_count:
            self.percentile_to_iterate_to = 100.0
            self.reached_last_recorded_value = True
            return True
        return False

    def increment_iteration_level(self):
        self.percentile_to_iterate_from = self.percentile_to_iterate_to
        percentile_gap = 100.0 - (self.percentile_to_iterate_to)
        if percentile_gap:
            half_distance = math.pow(2, (math.log(100 / percentile_gap) / math.log(2)) + 1)
            percentile_reporting_ticks = self.percentile_ticks_per_half_distance * half_distance
            self.percentile_to_iterate_to += 100.0 / percentile_reporting_ticks

    def reached_iteration_level(self):
        if self.count_at_this_value == 0:
            return False
        current_percentile = (100.0 * self.total_count_to_current_index) / self.total_count
        return current_percentile >= self.percentile_to_iterate_to

    def get_percentile_iterated_to(self):
        return self.percentile_to_iterate_to

    def get_percentile_iterated_from(self):
        return self.percentile_to_iterate_from
