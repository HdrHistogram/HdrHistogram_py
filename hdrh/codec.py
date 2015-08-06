'''
A pure python version of the hdr_histogram code

Ported from
https://github.com/HdrHistogram/HdrHistogram (Java)
https://github.com/HdrHistogram/HdrHistogram_c (C)

Coding / Decoding of histograms using the
HdrHistogram V1 compressed transportable format

Written by Alec Hothan
Apache License 2.0

'''
from ctypes import BigEndianStructure
from ctypes import c_int
from ctypes import c_longlong
from ctypes import sizeof
from StringIO import StringIO as StringIo
import ctypes
import base64
import zlib
import cStringIO

V1_ENCODING_COOKIE = 0x1c849301 + (8 << 4)
V1_COMPRESSION_COOKIE = 0x1c849302 + (8 << 4)
HDR_COOKIE_V1 = 0x1c849301
COUNTS_SIZE = 1 + 1

class HdrCookieException(Exception):
    pass

class HdrLengthException(Exception):
    pass

class HdrHistogramSettingsException(Exception):
    pass

class HdrHeader(BigEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("cookie", c_int),
        ("length", c_int)
    ]

    def __init__(self):
        self.cookie = V1_COMPRESSION_COOKIE

class HdrDecompressedPayload(object):
    '''A class to decompress, keep a reference to the decompressed string and
    provide access to the counts arrays of the decompressed payload
    '''
    def __init__(self, payload_class, hist=None):
        self.hist = hist
        self.cookie = 0
        self.payload_len = 0
        self.normalizing_index_offset = 0
        self.significant_figures = 0
        self.lowest_trackable_value = 0
        self.highest_trackable_value = 0
        self.conversion_ratio_bits = 0
        self.counts = None
        self.dcdata = None
        self.payload_ptr = ctypes.POINTER(payload_class)

    def decompress(self, compressed_payload):
        '''Decompress a compressed payload into self

        Args:
            compressed_payload (string) a payload in zlib compressed form
        Exception:
            HdrCookieException:
                the compressed payload has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            HdrHistogramSettingsException:
                mismatch in the significant figures, lowest and highest
                         trackable value
        '''
        # Here it is important to keep a reference to the decompressed
        # string so that it does not get garbage collected
        self.dcdata = zlib.decompress(compressed_payload)
        # Now safely typecast to the decompressed string
        payload = ctypes.cast(self.dcdata, self.payload_ptr).contents
        len_dcdata = len(self.dcdata)
        # sanity check on the length
        # the payload length must match the size of the decompressed string
        if (len_dcdata != payload.payload_len):
            raise HdrLengthException()

        counts_array_len = len_dcdata - HdrPayloadFactory.get_payload_header_size()
        # check the decompressed size is valid
        if counts_array_len < 0 or counts_array_len > sizeof(payload.counts) or \
           counts_array_len % sizeof(c_longlong):
            raise HdrLengthException()

        # Emulate all the fields of a HdrPayload<size> class
        self.cookie = payload.cookie
        self.payload_len = payload.payload_len
        self.normalizing_index_offset = payload.normalizing_index_offset
        self.significant_figures = payload.significant_figures
        self.lowest_trackable_value = payload.lowest_trackable_value
        self.highest_trackable_value = payload.highest_trackable_value
        self.conversion_ratio_bits = payload.conversion_ratio_bits
        # point directly to the decompressed counts array (no copy)
        self.counts = payload.counts

        if self.cookie != V1_ENCODING_COOKIE:
            raise HdrCookieException()
        # verify the cookie and other parameters match
        hist = self.hist
        if hist:
            if hist.significant_figures != self.significant_figures or \
               hist.lowest_trackable_value != self.lowest_trackable_value or \
               hist.highest_trackable_value != self.highest_trackable_value:
                raise HdrHistogramSettingsException()

    def get_decompressed_counters_count(self):
        '''Certain decompressed payload objects can have a counts array that has
        less usable elements than the class-defined array size if
        they have been decompressed from a histogram that has been
        compressed with partial counts.
        In this case access to the counts arrays must be limited
        to at most the index returned by this function.

        Returns:
            the number of counters present in the counts array of this instance
        '''
        # We simply deduct this number from the payload length
        counts_array_len = self.payload_len - HdrPayloadFactory.get_payload_header_size()
        return counts_array_len / sizeof(c_longlong)

    def dump(self):
        dump_payload(self.counts, self.payload_len)

class HdrPayloadFactory(object):
    # a dict of HdrPayload classes indexed by the number of counters in the class
    payload_classes = {}
    # size of the payload header in bytes (do not count the counters array)
    # is the size of the class with zero counters
    # this is initialized at first use in the decompress static method
    # since we cannot refer to HdrPayloadFactory yet at this point
    payload_header_size = 0

    @staticmethod
    def get_class(counters_count):
        '''A factory function to get an HdrPayload class
        The class needs to be created at runtime because the number of counters
        (or buckets) depends on the characteristics of the histogram

        Args:
            counters_count number of 64-bit counters in the array of counters
        '''
        if counters_count < 0:
            counters_count = 0

        hdr_payload_classes = HdrPayloadFactory.payload_classes
        if counters_count not in hdr_payload_classes:
            # Create a new class for this counters count
            class NewHdrPayload(BigEndianStructure):
                '''We only support the V1 encoding
                '''
                _pack_ = 1
                _fields_ = [
                    ("cookie", c_int),
                    ("payload_len", c_int),
                    ("normalizing_index_offset", c_int),
                    ("significant_figures", c_int),
                    ("lowest_trackable_value", c_longlong),
                    ("highest_trackable_value", c_longlong),
                    ("conversion_ratio_bits", c_longlong),
                    ("counts", c_longlong * counters_count)
                ]

                def compress(self, encode_counters_count=-1):
                    '''Compress this payload instance

                    Args:
                        encode_counters_count how many counters should be encoded
                                       starting from index 0 (can be 0)
                                       Negative value means compress all counters
                    Return:
                        the compressed payload (python string)
                    '''
                    if encode_counters_count >= 0:
                        self.payload_len = HdrPayloadFactory.get_payload_header_size() + \
                            encode_counters_count * sizeof(c_longlong)
                    else:
                        self.payload_len = sizeof(self)
                    data = ctypes.string_at(ctypes.byref(self), self.payload_len)
                    return zlib.compress(data)

                def add(self, dpayload, hist):
                    '''Add counters of a decompressed payload to this payload
                    and update the histogram total count, min, max
                    '''
                    counters_count = dpayload.get_decompressed_counters_count()

                    if counters_count:
                        # faster to get rid of all field accesses using dots
                        to_counts = self.counts
                        from_counts = dpayload.counts
                        total_added = 0
                        min_non_zero_index = -1
                        max_index = -1
                        for index in xrange(counters_count):
                            delta = from_counts[index]
                            if delta:
                                to_counts[index] += delta
                                total_added += delta
                                max_index = index
                                if min_non_zero_index < 0 and index:
                                    min_non_zero_index = index
                        if hist:
                            hist.adjust_internal_tacking_values(min_non_zero_index,
                                                                max_index,
                                                                total_added)

                def dump(self):
                    dump_payload(self.counts, sizeof(self))

            # Rename the class after the counters count
                    # set the class name to reflect the count
            NewHdrPayload.__name__ = 'HdrPayload' + str(counters_count)
            hdr_payload_classes[counters_count] = NewHdrPayload

        return hdr_payload_classes[counters_count]

    @staticmethod
    def create_instance(counters_count):
        '''Create an instance of a payload class of a given counters count that
        is compressible
        '''
        instance = HdrPayloadFactory.get_class(counters_count)()
        instance.cookie = V1_ENCODING_COOKIE
        return instance

    @staticmethod
    def get_payload_header_size():
        '''Get the size of the payload header (or the size of a payload instance
        of a class that has zero counters)
        '''
        if not HdrPayloadFactory.payload_header_size:
            HdrPayloadFactory.payload_header_size = sizeof(HdrPayloadFactory.get_class(0))
        return HdrPayloadFactory.payload_header_size

    @staticmethod
    def decompress(compressed_payload, payload_class):
        '''Decompress a compressed payload into an instance of the given
        payload class. Note that the resulting payload may have fewer
        counters than the total number of counters in that class (and may even
        have zero counters if the origin histogram counters array is all zeros)
        Args:
            compressed_payload (string) a compressed payload
                               (as returned by zlib.compress())
            payload_class the HdrPayload class to use for the results
        Returns:
            an instance of the HdrPayload class that contains the decompressed payload
            the number of counters decompressed in the counts array must be
            obtained using the get_decompressed_counters_count() method.
            Note that the returned instance is not compressible
        Exception:
            HdrCookieException:
                the compressed payload has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            zlib.error:
                in case of zlib decompression error
        '''
        dpayload = HdrDecompressedPayload(payload_class)
        dpayload.decompress(compressed_payload)
        return dpayload
        # Note we cannot use something like
        # dpayload = payload_class.from_buffer(bytearray(dcdata))
        # because the decompressed size may be smaller than the number of
        # counters in the provided class and from_value will raise
        # ValueError: Buffer size too small

class HdrStringIO(StringIo):
    '''The purpose of this read only stream is to provide bytes from 2
    objects in sequence without copying any bytes.
    It is meant to be provided as an input to base64.encode() so we can get the
    b64 encoded string for the 2 objects
    Args:
        hdr a ctypes structure object
        payload a string containing the compressed payload (as returned by
                zlib.compress()) but could be any string for this class
    '''
    def __init__(self, hdr, compressed_payload):
        self.hdr_str = ctypes.string_at(ctypes.byref(hdr), sizeof(hdr))
        StringIo.__init__(self, compressed_payload)

    def read(self, count=-1):
        if self.hdr_str:
            if len(self.hdr_str) <= count:
                hdr_str = self.hdr_str
                self.hdr_str = None
            else:
                # should never happen since hdr should be pretty small
                # so make this simple even if unoptimized
                hdr_str = self.hdr_str[:count]
                self.hdr_str = self.hdr_str[count:]
            return hdr_str
        res = StringIo.read(self, 4)
        return res

class HdrHistogramEncoder(object):
    '''An encoder class for histograms, only supports V1 encoding.
    The purpose of this encoder is to hold all the resources related to
    encoding and decoding so they are not re-allocated for every encode/decode.

    self.payload holds an encodable payload which can be used to store
    live counters that can be encoded efficiently

    self.dpayload holds a HdrDecompressPayload instance that can be reused
    to decompress multiple histograms
    '''
    def __init__(self, histogram, b64_wrap=True):
        '''Histogram encoder
        Args:
            histogram the histogram to encode/decode into
            b64_wrap determines if the base64 wrapper is enabled or not
        '''
        self.histogram = histogram
        self.header = HdrHeader()
        self.payload = HdrPayloadFactory.create_instance(histogram.counts_len)
        # those values never change across encodings
        self.payload.normalizing_index_offset = 0
        self.payload.conversion_ratio_bits = 1
        self.payload.significant_figures = histogram.significant_figures
        self.payload.lowest_trackable_value = histogram.lowest_trackable_value
        self.payload.highest_trackable_value = histogram.highest_trackable_value
        # where to decompress a new payload
        self.dpayload = HdrDecompressedPayload(self.payload.__class__, histogram)
        self.b64_wrap = b64_wrap

    def get_counts(self):
        '''Retrieve the counts array that can be used to store live counters
        and that can be encoded with minimal copies using encode()
        '''
        return self.payload.counts

    def encode(self):
        '''Compress the associated encodable payload,
        prepend the header then encode with base64 if requested

        Returns:
            the b64 encoded wire encoding of the histogram (as a string)
            or the compressed payload (as a string, if b64 wrappinb is disabled)
        '''
        # only compress the first non zero buckets
        relevant_length = \
            self.histogram.get_counts_array_index(self.histogram.max_value) + 1
        cpayload = self.payload.compress(relevant_length)
        if self.b64_wrap:
            self.header.length = len(cpayload)
            hdr_stringio = HdrStringIO(self.header, cpayload)
            output = cStringIO.StringIO()
            # unfortunately this API only encodes using the "standard" base64
            # format (one with lines limited to 57 characters and with CRLF)
            base64.encode(hdr_stringio, output)
            return output.getvalue()
        return cpayload

    def _decode_b64(self, encoded_histogram):
        '''Decode the base64 string and verify header sanity
        Return:
            a string containing the compresse payload
        Exception:
            TypeError in case of base64 decode error
            HdrCookieException:
                the main header has an invalid cookie
                the compressed payload header has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
        '''
        if self.b64_wrap:
            b64decode = base64.b64decode(encoded_histogram)
            # this string has 2 parts in it: the header (raw) and the payload (compressed)

            # use typecast to point to the start of the decode string with an HdrHeader pointer
            header = ctypes.cast(b64decode, ctypes.POINTER(HdrHeader)).contents
            if header.cookie != V1_COMPRESSION_COOKIE:
                raise HdrCookieException()
            if header.length != len(b64decode) - sizeof(header):
                raise HdrLengthException()
            # this will result in a copy of the compressed payload part
            # could not find a way to do otherwise since zlib.decompress()
            # expects a string (and does not like a buffer or a memoryview object)
            return b64decode[sizeof(HdrHeader):]
        # no base64 encoding
        return encoded_histogram

    def decode(self, encoded_histogram):
        '''Decode a wire histogram encoding
        Args:
            encoded_histogram a string containing the wire encoding of a histogram
                              such as one returned from encode()
        Returns:
            an HdrPayload instance with all the decoded/uncompressed fields

        Exception:
            TypeError in case of base64 decode error
            HdrCookieException:
                the main header has an invalid cookie
                the compressed payload header has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            HdrHistogramSettingsException:
                mismatch in the significant figures, lowest and highest
                         trackable value
            zlib.error:
                in case of zlib decompression error
        '''
        cpayload = self._decode_b64(encoded_histogram)
        dpayload = HdrDecompressedPayload(self.payload.__class__, self.histogram)
        dpayload.decompress(cpayload)
        return dpayload

    def decode_and_add(self, encoded_histogram):
        '''Decode a wire format histogram encoding and add it to the associated
        histogram. This is a more efficient version of decode() as counters
        are directly added to the histogram counts array

        Args:
            encoded_histogram a string containing the wire encoding of a histogram
                              such as one returned from encode()

        Exception:
            TypeError in case of base64 decode error
            HdrCookieException:
                the main header has an invalid cookie
                the compressed payload header has an invalid cookie
            HdrLengthException:
                the decompressed size is too small for the HdrPayload structure
                or is not aligned or is too large for the passed payload class
            HdrHistogramSettingsException:
                mismatch in the significant figures, lowest and highest
                         trackable value
            zlib.error:
                in case of zlib decompression error
        '''
        cpayload = self._decode_b64(encoded_histogram)
        self.dpayload.decompress(cpayload)
        self.payload.add(self.dpayload, self.histogram)

def _dump_series(start, stop, count):
    if stop <= start + 1:
        # single index range
        print '[%06d] %d' % (start, count)
    else:
        print '[%06d] %d (%d identical)' % (start, count, stop - start)

def dump_payload(counts, payload_len):
    max_index = (payload_len - HdrPayloadFactory.get_payload_header_size()) / sizeof(c_longlong)
    print 'counts array size = %d entries' % (max_index)
    if not max_index:
        return
    series_start_index = 0
    current_series_count = counts[0]
    index = 1
    for index in xrange(1, max_index):
        if counts[index] != current_series_count:
            # dump the current series
            _dump_series(series_start_index, index, current_series_count)
            # start a new series
            current_series_count = counts[index]
            series_start_index = index
    # there is always a last series to dump
    _dump_series(series_start_index, index, counts[index])
    print '[%06d] --END--' % (index + 1)
