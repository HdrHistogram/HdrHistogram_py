
import math

class HdrIterator(object):
    '''Provide a means of iterating through all histogram values using the finest
    granularity steps supported by the underlying representation.
    The iteration steps through all possible unit value levels, regardless of
    whether or not there were recorded values for that value level,
    and terminates when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram):
        self.histogram = histogram
        self.bucket_index = 0
        self.sub_bucket_index = -1
        self.count_at_index = 0
        self.count_to_index = 0
        self.value_from_index = 0
        self.highest_equivalent_value = 0

    def __iter__(self):
        self.bucket_index = 0
        self.sub_bucket_index = -1
        self.count_at_index = 0
        self.count_to_index = 0
        self.value_from_index = 0
        self.highest_equivalent_value = 0
        return self

    def has_next(self):
        return self.count_to_index < self.histogram.total_count

    def move_next(self):
        if self.has_next():
            self.sub_bucket_index += 1
            if self.sub_bucket_index >= self.histogram.sub_bucket_count:
                self.sub_bucket_index = self.histogram.sub_bucket_half_count
                self.bucket_index += 1
            self.count_at_index = self.histogram.get_count_at_index(self.bucket_index,
                                                                    self.sub_bucket_index)
            self.count_to_index += self.count_at_index
        else:
            raise StopIteration()

    def next(self):
        self.move_next()
        return self.update_values()

    def update_values(self):
        self.value_from_index = self.histogram.get_value_from_index(self.bucket_index,
                                                                    self.sub_bucket_index)
        self.highest_equivalent_value = \
            self.histogram.get_highest_equivalent_value(self.value_from_index)
        return self.highest_equivalent_value

class HdrRecordedIterator(HdrIterator):
    '''Provide a means of iterating through all recorded histogram values
    using the finest granularity steps supported by the underlying representation.
    The iteration steps through all non-zero recorded value counts,
    and terminates when all recorded histogram values are exhausted.
    '''
    def next(self):
        while True:
            self.move_next()
            if self.count_at_index:
                break
        return self.update_values()

class HdrLiLoIterator(HdrIterator):
    '''Linear/Log iterator common parent class
    '''
    def __init__(self, histogram, next_value_report_lev):
        HdrIterator.__init__(self, histogram)
        self.next_value_report_lev = next_value_report_lev
        self.next_value_report_lev_lowest_eq = \
            histogram.get_lowest_equivalent_value(next_value_report_lev)
        self.count_added_in_this_iter_step = 0

    def move_next_value(self):
        pass

    def peek_next_value_from_index(self):
        # get the indices for teh next bucket
        bucket_index = self.bucket_index
        sub_bucket_index = self.sub_bucket_index + 1
        if sub_bucket_index >= self.histogram.sub_bucket_count:
            sub_bucket_index = self.histogram.sub_bucket_half_count
            bucket_index += 1
        return self.histogram.get_value_from_index(bucket_index, sub_bucket_index)

    def next(self):
        self.count_added_in_this_iter_step = 0
        if self.has_next() or \
           self.peek_next_value_from_index() > self.next_value_report_lev_lowest_eq:
            while True:
                if self.value_from_index >= self.next_value_report_lev_lowest_eq or \
                   not self.has_next():
                    self.move_next_value()
                    self.next_value_report_lev_lowest_eq = \
                        self.histogram.get_lowest_equivalent_value(self.next_value_report_lev)
                    return self.highest_equivalent_value
                self.move_next()
                self.update_values()
                self.count_added_in_this_iter_step += self.count_at_index
        raise StopIteration()

class HdrLinearIterator(HdrLiLoIterator):
    '''Provide a means of iterating through histogram values using linear steps.

    The iteration is performed in steps of value_units_per_bucket in size,
    terminating when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, value_units_per_bucket):
        HdrLiLoIterator.__init__(self, histogram, value_units_per_bucket)
        self.value_units_per_bucket = value_units_per_bucket

    def move_next_value(self):
        self.next_value_report_lev += self.value_units_per_bucket

class HdrLogIterator(HdrLiLoIterator):
    '''Provide a means of iterating through histogram values at logarithmically
    increasing levels.

    The iteration is performed in steps that start at value_units_first_bucket
    and increase exponentially according to log_base, terminating when all
    recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, value_units_first_bucket, log_base):
        HdrLiLoIterator.__init__(self, histogram, value_units_first_bucket)
        self.log_base = log_base

    def move_next_value(self):
        self.next_value_report_lev *= self.log_base

class HdrPercentileIterator(HdrIterator):
    '''Provide a means of iterating through histogram values according to
    percentile levels

    The iteration is performed in steps that start at 0% and reduce their
    distance to 100% according to the ticks_per_half_distance parameter,
    ultimately reaching 100% when all recorded histogram values are exhausted.
    '''
    def __init__(self, histogram, ticks_per_half_distance):
        HdrIterator.__init__(self, histogram)
        self.ticks_per_half_distance = ticks_per_half_distance
        self.percentile_to_iterate_to = 0
        self.target_count_at_percentile = 0
        self.percentile = 0
        self.seen_last_value = False
        self.total_count = self.histogram.get_total_count()

    def _ret_last(self):
        if self.seen_last_value:
            raise StopIteration()
        self.seen_last_value = True
        self.percentile = 100.0
        return self.highest_equivalent_value

    def next(self):
        if not self.has_next():
            return self._ret_last()

        # on first iteration move to next
        if self.sub_bucket_index == -1:
            self.move_next()

        while True:
            if self.count_at_index:
                # Note that this test
                # current_percentile = (100.0 * self.count_to_index) / self.total_count
                # if current_percentile >= self.percentile_to_iterate_to:
                # will not work in all rounding cases because of how the target
                # count is calculated with a + 0.5
                # So we need to test the same way as get_value_at_percentile
                # so that the UT will show the same value between
                # a percentile iteration and a direct get_value_at_percentile
                if self.count_to_index >= self.target_count_at_percentile:
                    self.percentile = self.percentile_to_iterate_to
                    half_distance = math.pow(2,
                                             (math.log(100 /
                                                       (100.0 - (self.percentile_to_iterate_to)))
                                              /
                                              math.log(2)) + 1)
                    percentile_reporting_ticks = self.ticks_per_half_distance * half_distance
                    self.percentile_to_iterate_to += 100.0 / percentile_reporting_ticks
                    self.target_count_at_percentile = \
                        self.histogram.get_target_count_at_percentile(self.percentile_to_iterate_to)
                    return self.update_values()
            self.move_next()
