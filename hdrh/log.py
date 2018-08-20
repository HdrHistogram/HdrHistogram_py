'''
A histogram log writer.
A Histogram logs are used to capture full fidelity, per-time-interval
histograms of a recorded value.

For example, a histogram log can be used to capture high fidelity
reaction-time logs for some measured system or subsystem component.
Such a log would capture a full reaction time histogram for each
logged interval, and could be used to later reconstruct a full
HdrHistogram of the measured reaction time behavior for any arbitrary
time range within the log, by adding [only] the relevant interval
histograms.


Ported from
https://github.com/HdrHistogram/HdrHistogram (Java)

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
import datetime
import re
import sys
from hdrh.histogram import HdrHistogram

class HistogramLogWriter(object):

    HISTOGRAM_LOG_FORMAT_VERSION = "1.2"

    def __init__(self, output_file):
        '''Constructs a new HistogramLogWriter that will write into the specified file.
        Params:
            output_file the File to write to
        '''
        self.log = output_file
        self.base_time = 0

    def output_interval_histogram(self,
                                  histogram,
                                  start_time_stamp_sec=0,
                                  end_time_stamp_sec=0,
                                  max_value_unit_ratio=1000000.0):
        '''Output an interval histogram, with the given timestamp and a
        configurable maxValueUnitRatio.
        (note that the specified timestamp will be used, and the timestamp in
        the actual histogram will be ignored).
        The max value reported with the interval line will be scaled by the
        given max_value_unit_ratio.
        The histogram start and end timestamps are assumed to be in msec units.
        Logging will be in seconds, realtive by a base time
        The default base time is 0.

        By covention, histogram start/end time are generally stamped with
        absolute times in msec since the epoch. For logging with absolute time
        stamps, the base time would remain zero. For
        logging with relative time stamps (time since a start point),
        Params:
            histogram The interval histogram to log.
            start_time_stamp_sec The start timestamp to log with the
                interval histogram, in seconds.
                default: using the start/end timestamp indicated in the histogram
            end_time_stamp_sec The end timestamp to log with the interval
                histogram, in seconds.
                default: using the start/end timestamp indicated in the histogram
            max_value_unit_ratio The ratio by which to divide the histogram's max
                value when reporting on it.
                default: 1,000,000 (which is the msec : nsec ratio
        '''
        if not start_time_stamp_sec:
            start_time_stamp_sec = \
                (histogram.get_start_time_stamp() - self.base_time) / 1000.0
        if not end_time_stamp_sec:
            end_time_stamp_sec = (histogram.get_end_time_stamp() - self.base_time) / 1000.0
        cpayload = histogram.encode()
        self.log.write("%f,%f,%f,%s\n" %
                       (start_time_stamp_sec,
                        end_time_stamp_sec - start_time_stamp_sec,
                        histogram.get_max_value() // max_value_unit_ratio,
                        cpayload.decode('utf-8')))

    def output_start_time(self, start_time_msec):
        '''Log a start time in the log.
        Params:
            start_time_msec time (in milliseconds) since the absolute start time (the epoch)
        '''
        self.log.write("#[StartTime: %f (seconds since epoch), %s]\n" %
                       (float(start_time_msec) / 1000.0,
                        datetime.fromtimestamp(start_time_msec).iso_format(' ')))

    def output_base_time(self, base_time_msec):
        '''Log a base time in the log.
        Params:
            base_time_msec time (in milliseconds) since the absolute start time (the epoch)
        '''
        self.log.write("#[BaseTime: %f (seconds since epoch)]\n" %
                       (float(base_time_msec) / 1000.0))

    def output_comment(self, comment):
        '''Log a comment to the log.
        Comments will be preceded with with the '#' character.
        Params:
            comment the comment string.
        '''
        self.log.write("#%s\n" % (comment))

    def output_legend(self):
        '''Output a legend line to the log.
        '''
        self.log.write("\"StartTimestamp\",\"Interval_Length\","
                       "\"Interval_Max\",\"Interval_Compressed_Histogram\"\n")

    def output_log_format_version(self):
        '''Output a log format version to the log.
        '''
        self.output_comment("[Histogram log format version " +
                            HistogramLogWriter.HISTOGRAM_LOG_FORMAT_VERSION + "]")

    def close(self):
        self.log.close()


# "#[StartTime: %f (seconds since epoch), %s]\n"
re_start_time = re.compile(r'#\[StartTime: *([\d\.]*) ')

# "#[BaseTime: %f (seconds since epoch)]\n"
re_base_time = re.compile(r'#\[BaseTime: *([\d\.]*) ')

# "%f,%f,%f,%s\n"
re_histogram_interval = re.compile(r'([\d\.]*),([\d\.]*),([\d\.]*),(.*)')

class HistogramLogReader(object):

    def __init__(self, input_file_name, reference_histogram):
        '''Constructs a new HistogramLogReader that produces intervals read
        from the specified file name.
        Params:
            input_file_name The name of the file to read from
            reference_histogram a histogram instance used as a reference to create
                                new instances for all subsequent decoded interval
                                histograms
        '''
        self.start_time_sec = 0.0
        self.observed_start_time = False
        self.base_time_sec = 0.0
        self.observed_base_time = False
        self.input_file = open(input_file_name, "r")
        self.reference_histogram = reference_histogram

    def get_start_time_sec(self):
        '''get the latest start time found in the file so far (or 0.0),
        per the log file format explained above. Assuming the "#[StartTime:" comment
        line precedes the actual intervals recorded in the file, getStartTimeSec() can
        be safely used after each interval is read to determine's the offset of that
        interval's timestamp from the epoch.
        Return:
            latest Start Time found in the file (or 0.0 if non found)
        '''
        return self.start_time_sec

    def _decode_next_interval_histogram(self,
                                        dest_histogram,
                                        range_start_time_sec=0.0,
                                        range_end_time_sec=sys.maxsize,
                                        absolute=False):
        '''Read the next interval histogram from the log, if interval falls
        within an absolute or relative time range.

        Timestamps are assumed to appear in order in the log file, and as such
        this method will return a null upon encountering a timestamp larger than
        range_end_time_sec.

        Relative time range:
            the range is assumed to be in seconds relative to
            the actual timestamp value found in each interval line in the log
        Absolute time range:
            Absolute timestamps are calculated by adding the timestamp found
            with the recorded interval to the [latest, optional] start time
            found in the log. The start time is indicated in the log with
            a "#[StartTime: " followed by the start time in seconds.

        Params:
            dest_histogram if None, created a new histogram, else adds
                           the new interval histogram to it
            range_start_time_sec The absolute or relative start of the expected
                                 time range, in seconds.
            range_start_time_sec The absolute or relative end of the expected
                                  time range, in seconds.
            absolute Defines if the passed range is absolute or relative

        Return:
            Returns an histogram object if an interval line was found with an
            associated start timestamp value that falls between start_time_sec and
            end_time_sec,
            or null if no such interval line is found.
            Upon encountering any unexpected format errors in reading the next
            interval from the file, this method will return None.

            The histogram returned will have it's timestamp set to the absolute
            timestamp calculated from adding the interval's indicated timestamp
            value to the latest [optional] start time found in the log.

        Exceptions:
            ValueError if there is a syntax error in one of the float fields
        '''
        while 1:
            line = self.input_file.readline()
            if not line:
                return None
            if line[0] == '#':
                match_res = re_start_time.match(line)
                if match_res:
                    self.start_time_sec = float(match_res.group(1))
                    self.observed_start_time = True
                    continue
                match_res = re_base_time.match(line)
                if match_res:
                    self.base_time_sec = float(match_res.group(1))
                    self.observed_base_time = True
                    continue

            match_res = re_histogram_interval.match(line)
            if not match_res:
                # probably a legend line that starts with "\"StartTimestamp"
                continue
            # Decode: startTimestamp, intervalLength, maxTime, histogramPayload
            # Timestamp is expected to be in seconds
            log_time_stamp_in_sec = float(match_res.group(1))
            interval_length_sec = float(match_res.group(2))
            cpayload = match_res.group(4)

            if not self.observed_start_time:
                # No explicit start time noted. Use 1st observed time:
                self.start_time_sec = log_time_stamp_in_sec
                self.observed_start_time = True

            if not self.observed_base_time:
                # No explicit base time noted.
                # Deduce from 1st observed time (compared to start time):
                if log_time_stamp_in_sec < self.start_time_sec - (365 * 24 * 3600.0):
                    # Criteria Note: if log timestamp is more than a year in
                    # the past (compared to StartTime),
                    # we assume that timestamps in the log are not absolute
                    self.base_time_sec = self.start_time_sec
                else:
                    # Timestamps are absolute
                    self.base_time_sec = 0.0
                self.observed_base_time = True

            absolute_start_time_stamp_sec = \
                log_time_stamp_in_sec + self.base_time_sec
            offset_start_time_stamp_sec = \
                absolute_start_time_stamp_sec - self.start_time_sec

            # Timestamp length is expect to be in seconds
            absolute_end_time_stamp_sec = \
                absolute_start_time_stamp_sec + interval_length_sec

            if absolute:
                start_time_stamp_to_check_range_on = absolute_start_time_stamp_sec
            else:
                start_time_stamp_to_check_range_on = offset_start_time_stamp_sec

            if start_time_stamp_to_check_range_on < range_start_time_sec:
                continue

            if start_time_stamp_to_check_range_on > range_end_time_sec:
                return None
            if dest_histogram:
                # add the interval histogram to the destination histogram
                histogram = dest_histogram
                histogram.decode_and_add(cpayload)
            else:
                histogram = HdrHistogram.decode(cpayload)
                histogram.set_start_time_stamp(absolute_start_time_stamp_sec * 1000.0)
                histogram.set_end_time_stamp(absolute_end_time_stamp_sec * 1000.0)
            return histogram

    def get_next_interval_histogram(self,
                                    range_start_time_sec=0.0,
                                    range_end_time_sec=sys.maxsize,
                                    absolute=False):
        '''Read the next interval histogram from the log, if interval falls
        within an absolute or relative time range.

        Timestamps are assumed to appear in order in the log file, and as such
        this method will return a null upon encountering a timestamp larger than
        range_end_time_sec.

        Relative time range:
            the range is assumed to be in seconds relative to
            the actual timestamp value found in each interval line in the log
        Absolute time range:
            Absolute timestamps are calculated by adding the timestamp found
            with the recorded interval to the [latest, optional] start time
            found in the log. The start time is indicated in the log with
            a "#[StartTime: " followed by the start time in seconds.

        Params:

            range_start_time_sec The absolute or relative start of the expected
                                 time range, in seconds.
            range_start_time_sec The absolute or relative end of the expected
                                  time range, in seconds.
            absolute Defines if the passed range is absolute or relative

        Return:
            Returns an histogram object if an interval line was found with an
            associated start timestamp value that falls between start_time_sec and
            end_time_sec,
            or null if no such interval line is found.
            Upon encountering any unexpected format errors in reading the next
            interval from the file, this method will return None.

            The histogram returned will have it's timestamp set to the absolute
            timestamp calculated from adding the interval's indicated timestamp
            value to the latest [optional] start time found in the log.

        Exceptions:
            ValueError if there is a syntax error in one of the float fields
        '''
        return self._decode_next_interval_histogram(None,
                                                    range_start_time_sec,
                                                    range_end_time_sec,
                                                    absolute)

    def add_next_interval_histogram(self,
                                    dest_histogram=None,
                                    range_start_time_sec=0.0,
                                    range_end_time_sec=sys.maxsize,
                                    absolute=False):
        '''Read the next interval histogram from the log, if interval falls
        within an absolute or relative time range, and add it to the destination
        histogram (or to the reference histogram if dest_histogram is None)

        Timestamps are assumed to appear in order in the log file, and as such
        this method will return a null upon encountering a timestamp larger than
        range_end_time_sec.

        Relative time range:
            the range is assumed to be in seconds relative to
            the actual timestamp value found in each interval line in the log
        Absolute time range:
            Absolute timestamps are calculated by adding the timestamp found
            with the recorded interval to the [latest, optional] start time
            found in the log. The start time is indicated in the log with
            a "#[StartTime: " followed by the start time in seconds.

        Params:
            dest_histogram where to add the next interval histogram, if None
                           the interal histogram will be added to the reference
                           histogram passed in the constructor
            range_start_time_sec The absolute or relative start of the expected
                                 time range, in seconds.
            range_start_time_sec The absolute or relative end of the expected
                                  time range, in seconds.
            absolute Defines if the passed range is absolute or relative

        Return:
            Returns the destination histogram if an interval line was found with an
            associated start timestamp value that falls between start_time_sec and
            end_time_sec,
            or None if no such interval line is found.
            Upon encountering any unexpected format errors in reading the next
            interval from the file, this method will return None.

            The histogram returned will have it's timestamp set to the absolute
            timestamp calculated from adding the interval's indicated timestamp
            value to the latest [optional] start time found in the log.

        Exceptions:
            ValueError if there is a syntax error in one of the float fields
        '''
        if not dest_histogram:
            dest_histogram = self.reference_histogram
        return self._decode_next_interval_histogram(dest_histogram,
                                                    range_start_time_sec,
                                                    range_end_time_sec,
                                                    absolute)

    def close(self):
        self.input_file.close()
