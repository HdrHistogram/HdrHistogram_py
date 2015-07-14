'''
A port to python of the hdr_histogram.c code that is included in
https://github.com/giltene/wrk2.git
'''
import math
import sys

def get_bucket_count(value, subb_count, unit_mag):
    smallest_untrackable_value = subb_count << unit_mag
    buckets_needed = 1
    while smallest_untrackable_value <= value:
        if smallest_untrackable_value > sys.maxint / 2:
            return buckets_needed + 1
        smallest_untrackable_value <<= 1
        buckets_needed += 1
    return buckets_needed

class HdrIterator(object):
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

    def next(self):
        if self.count_to_index >= self.histogram.total_count:
            raise StopIteration()
        self.sub_bucket_index += 1
        if self.sub_bucket_index >= self.histogram.sub_bucket_count:
            self.sub_bucket_index = self.histogram.sub_bucket_half_count
            self.bucket_index += 1
        self.count_at_index = self.histogram.get_count_at_index(self.bucket_index,
                                                                self.sub_bucket_index)
        self.count_to_index += self.count_at_index
        self.value_from_index = self.histogram.get_value_from_index(self.bucket_index,
                                                                    self.sub_bucket_index)
        # self.highest_equivalent_value = 0
        return self.value_from_index

class HdrHistogram(object):

    def __init__(self,
                 lowest_trackable_value,
                 highest_trackable_value,
                 significant_figures):
        self.lowest_trackable_value = lowest_trackable_value
        self.highest_trackable_value = highest_trackable_value
        self.significant_figures = significant_figures
        self.unit_magnitude = int(math.floor(math.log(lowest_trackable_value) /
                                             math.log(2)))
        largest_value_single_unit_res = 2 * math.pow(10, significant_figures)
        subb_count_mag = int(math.ceil(math.log(largest_value_single_unit_res) /
                                       math.log(2)))
        self.sub_bucket_half_count_magnitude = subb_count_mag - 1 if subb_count_mag > 1 else 0
        self.sub_bucket_count = int(math.pow(2, self.sub_bucket_half_count_magnitude + 1))
        self.sub_bucket_half_count = self.sub_bucket_count / 2
        self.sub_bucket_mask = (self.sub_bucket_count - 1) << self.unit_magnitude
        self.bucket_count = get_bucket_count(highest_trackable_value,
                                             self.sub_bucket_count,
                                             self.unit_magnitude)
        self.counts_len = (self.bucket_count + 1) * (self.sub_bucket_count / 2)
        self.counts = [0] * self.counts_len
        self.total_count = 0
        self.min_value = sys.maxint
        self.max_value = 0

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

    def record_value(self, value):
        if value < 0:
            return False
        counts_index = self._counts_index_for(value)
        self.counts[counts_index] += 1
        self.total_count += 1
        # print 'record_value %d -> counts_index=%d' % (value, counts_index)
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.min_value, value)
        return True

    def get_count_at_index(self, bucket_index, sub_bucket_index):
        # Calculate the index for the first entry in the bucket:
        # (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) )
        bucket_base_index = (bucket_index + 1) << self.sub_bucket_half_count_magnitude
        # Calculate the offset in the bucket:
        offset_in_bucket = sub_bucket_index - self.sub_bucket_half_count
        # The following is the equivalent of
        # (sub_bucket_index - subBucketHalfCount) + bucketBaseIndex
        counts_index = bucket_base_index + offset_in_bucket
        return self.counts[counts_index]

    def get_value_from_index(self, bucket_index, sub_bucket_index):
        return sub_bucket_index << (bucket_index + self.unit_magnitude)

    def get_lowest_equivalent_value(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)

        lowest_equivalent_value = self.get_value_from_index(bucket_index,
                                                            sub_bucket_index)
        return lowest_equivalent_value

    def get_highest_equivalent_value(self, value):
        bucket_index = self._get_bucket_index(value)
        sub_bucket_index = self._get_sub_bucket_index(value, bucket_index)

        lowest_equivalent_value = self.get_value_from_index(bucket_index,
                                                            sub_bucket_index)
        if sub_bucket_index >= self.sub_bucket_count:
            bucket_index += 1
        size_of_equivalent_value_range = 1 << (self.unit_magnitude + bucket_index)
        next_non_equivalent_value = lowest_equivalent_value + size_of_equivalent_value_range

        return next_non_equivalent_value - 1

    def get_value_at_percentile(self, percentile):
        itr = HdrIterator(self)
        requested_percentile = percentile if percentile < 100.0 else 100.0
        count_at_percentile = int(((requested_percentile / 100) * self.total_count) + 0.5)
        count_at_percentile = max(count_at_percentile, 1)
        total = 0
        for value in itr:
            '''
            if itr.count_at_index:
                print('--- Iteration bsb=%d/%d index=%d value=%d at_index=%d to_index=%d' %
                      (itr.bucket_index,
                       itr.sub_bucket_index,
                       self._counts_index(itr.bucket_index, itr.sub_bucket_index),
                       value, itr.count_at_index, total))
            '''
            total += itr.count_at_index
            if total >= count_at_percentile:
                return self.get_highest_equivalent_value(value)
        return 0

    def get_percentile_to_value_dict(self, percentile_list):
        result = {}
        itr = HdrIterator(self)
        total = 0
        percentile_list_index = 0
        count_at_percentile = 0
        # remove dups and sort
        percentile_list = list(set(percentile_list))
        percentile_list.sort()
        for value in itr:
            total += itr.count_at_index
            while True:
                # recalculate target based on next requested percentile
                if not count_at_percentile:
                    if percentile_list_index == len(percentile_list):
                        return result
                    percentile = percentile_list[percentile_list_index]
                    percentile_list_index += 1
                    if percentile > 100:
                        return result
                    count_at_percentile = int(((percentile / 100) * self.total_count) + 0.5)
                    count_at_percentile = max(count_at_percentile, 1)
                if total >= count_at_percentile:
                    result[percentile] = self.get_highest_equivalent_value(value)
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
        return self.get_lowest_equivalent_value(val1) == self.get_lowest_equivalent_value(val2)

    def get_max_value(self):
        if 0 == self.max_value:
            return 0
        return self.get_highest_equivalent_value(self.max_value)

    def get_min_value(self):
        if 0 < self.counts[0]:
            return 0
        if sys.maxint == self.min_value:
            return sys.maxint
        return self.get_lowest_equivalent_value(self.min_value)

    def add_bucket_counts(self, bucket_counts):
        '''Add a list of bucket/sub-bucket counts to the histogram
        bucket_counts must be a dict with the following content:
        {"buckets":27, "sub_buckets": 2048, "digits": 3,
         "max_latency": 86400000000,
         "min": 891510,
         "max": 2097910,
         "counters":
            # list of bucket_index, [sub_bucket_index, count...]
            [12, [1203, 1, 1272, 1, 1277, 1, 1278, 1, 1296, 1],
             13, [1024, 1, 1027, 2, 1030, 1, 1031, 1]]
        }
        returns true if success or false if error (buckets and sub bucket count
        must match)
        '''
        if (bucket_counts['buckets'] != self.bucket_count) or \
           (bucket_counts['sub_buckets'] != self.sub_bucket_count):
            return False
        counts = bucket_counts['counters']
        for index in range(0, len(counts), 2):
            bucket_index = counts[index]
            sub_bucket_list = counts[index + 1]
            for sub_index in range(0, len(sub_bucket_list), 2):
                sub_bucket_index = sub_bucket_list[sub_index]
                counts_index = self._counts_index(bucket_index, sub_bucket_index)
                count = sub_bucket_list[sub_index + 1]
                self.counts[counts_index] += count
                self.total_count += count
        self.min_value = min(self.min_value, bucket_counts['min'])
        self.max_value = max(self.max_value, bucket_counts['max'])
        return True
