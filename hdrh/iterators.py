
class HdrIterator(object):
    '''A basic iterator that iterates through each bucket
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

    def move_next(self):
        if self.count_to_index >= self.histogram.total_count:
            raise StopIteration()
        self.sub_bucket_index += 1
        if self.sub_bucket_index >= self.histogram.sub_bucket_count:
            self.sub_bucket_index = self.histogram.sub_bucket_half_count
            self.bucket_index += 1
        self.count_at_index = self.histogram.get_count_at_index(self.bucket_index,
                                                                self.sub_bucket_index)
        self.count_to_index += self.count_at_index

    def next(self):
        self.move_next()
        self.value_from_index = self.histogram.get_value_from_index(self.bucket_index,
                                                                    self.sub_bucket_index)
        return self.value_from_index

class HdrRecordedIterator(HdrIterator):
    '''An iterator for iterating through each recorded value, will skip
    empty buckets
    '''
    def next(self):
        while True:
            self.move_next()
            if self.count_at_index:
                break

        self.value_from_index = self.histogram.get_value_from_index(self.bucket_index,
                                                                    self.sub_bucket_index)
        return self.value_from_index

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

    def next(self):
        self.count_added_in_this_iter_step = 0
        while True:
            self.move_next()
            self.value_from_index = self.histogram.get_value_from_index(self.bucket_index,
                                                                        self.sub_bucket_index)
            self.count_added_in_this_iter_step += self.count_at_index
            if (self.value_from_index >= self.next_value_report_lev_lowest_eq) or \
               (self.count_to_index >= self.histogram.total_count):
                self.move_next_value()
                self.next_value_report_lev_lowest_eq = \
                    self.histogram.get_lowest_equivalent_value(self.next_value_report_lev)
                return self.value_from_index

class HdrLinearIterator(HdrLiLoIterator):
    '''Linear iterator using a fixed value units per bucket
    '''
    def __init__(self, histogram, value_units_per_bucket):
        HdrLiLoIterator.__init__(self, histogram, value_units_per_bucket)
        self.value_units_per_bucket = value_units_per_bucket

    def move_next_value(self):
        self.next_value_report_lev += self.value_units_per_bucket

class HdrLogIterator(HdrLiLoIterator):
    '''Linear iterator using a fixed value units per bucket
    '''
    def __init__(self, histogram, value_units_first_bucket, log_base):
        HdrLiLoIterator.__init__(self, histogram, value_units_first_bucket)
        self.log_base = log_base

    def move_next_value(self):
        self.next_value_report_lev *= self.log_base
