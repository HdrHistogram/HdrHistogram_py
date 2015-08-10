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
from ctypes import c_ubyte
from ctypes import c_short
from ctypes import c_int
from ctypes import c_longlong
from ctypes import sizeof
import ctypes
import base64
import zlib

# For now we only use 8-byte counters (64-bits)
V1_ENCODING_COOKIE_BASE = 0x1c849301
V1_COMPRESSION_COOKIE_BASE = 0x1c849302

def get_cookie_base(cookie):
    return cookie & ~0xf0

def get_word_size_in_bytes_from_cookie(cookie):
    return (cookie & 0xf0) >> 4

def get_encoding_cookie(word_size):
    return V1_ENCODING_COOKIE_BASE | (word_size << 4)

def get_compression_cookie(word_size):
    return V1_COMPRESSION_COOKIE_BASE | (word_size << 4)

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

# V1 encoding header
payload_hdr_fields = [
    ("cookie", c_int),
    ("payload_len", c_int),
    ("normalizing_index_offset", c_int),
    ("significant_figures", c_int),
    ("lowest_trackable_value", c_longlong),
    ("highest_trackable_value", c_longlong),
    ("conversion_ratio_bits", c_longlong)]

# A maximum number of entries in the counts array
# This is to work around the index check done by the python runtime
MAX_COUNTS = 1000 * 1000

class HdrPayloadHeader(BigEndianStructure):
    _pack_ = 1
    _fields_ = payload_hdr_fields

payload_header_size = sizeof(HdrPayloadHeader)
payload_header_ptr = ctypes.POINTER(HdrPayloadHeader)

class HdrPayload64(BigEndianStructure):
    _pack_ = 1
    _fields_ = payload_hdr_fields + [("counts", c_longlong * MAX_COUNTS)]

class HdrPayload32(BigEndianStructure):
    _pack_ = 1
    _fields_ = payload_hdr_fields + [("counts", c_int * MAX_COUNTS)]

class HdrPayload16(BigEndianStructure):
    _pack_ = 1
    _fields_ = payload_hdr_fields + [("counts", c_short * MAX_COUNTS)]

# list of supported payload classes, indexed by the word size
payload_class_ptrs = [None, None,
                      ctypes.POINTER(HdrPayload16),   # index 2
                      None,
                      ctypes.POINTER(HdrPayload32),   # index 4
                      None, None, None,
                      ctypes.POINTER(HdrPayload64)]   # index 8


class HdrPayload(object):
    '''A class that wraps the ctypes big endian struct that will hold the
    histogram wire format content (including the counters).
    If word_size is non-zero, the space to store all the fields and the
    counters will be allocated.
    If word_size is 0, this wrapper will not allocate any space (used as an
    empty wrapper for decompressing into)
    Params:
        word_size counter size in bytes (2,4,8 byte counters are supported)
        counts_len number of counters
    '''
    def __init__(self, word_size=0, counts_len=0):
        self.word_size = word_size
        self.counts_len = counts_len
        if word_size:
            # we only support 2,4,8 as word sizes
            try:
                payload_class_ptr = payload_class_ptrs[word_size]
            except IndexError:
                raise ValueError('Invalid word size')
            if not payload_class_ptr:
                raise ValueError('Invalid word size')
            payload_len = payload_header_size + counts_len * word_size
            # allocate the memory for this payload
            self._data = (c_ubyte * payload_len)()
            # now that we now the word size, typecast with the right class ptr
            self.payload = ctypes.cast(self._data, payload_class_ptr).contents
            self.payload.cookie = get_encoding_cookie(word_size)
            self.counts_len = counts_len
        else:
            self._data = None
            self.payload = None
            self.payload_len = 0
            self.counts_len = 0

    def get_counts(self):
        return self.payload.counts

    def decompress(self, compressed_payload):
        '''Decompress a compressed payload into this empty payload wrapper

        Args:
            compressed_payload (string) a payload in zlib compressed form
        Return:
            self
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
        # make sure this instance is pristine
        if self._data:
            raise RuntimeError('Cannot decompress to an instance with payload')
        # Here it is important to keep a reference to the decompressed
        # string so that it does not get garbage collected
        self._data = zlib.decompress(compressed_payload)
        len_data = len(self._data)

        # Access the payload header
        payload = ctypes.cast(self._data, payload_header_ptr).contents

        cookie = payload.cookie
        if get_cookie_base(cookie) != V1_ENCODING_COOKIE_BASE:
            raise HdrCookieException('Invalid cookie')
        word_size = get_word_size_in_bytes_from_cookie(cookie)
        # we only support 2,4,8 as word sizes
        try:
            payload_class_ptr = payload_class_ptrs[word_size]
        except IndexError:
            raise HdrCookieException('Invalid word size')

        # now that we now the word size, typecast with the right class ptr
        payload = ctypes.cast(self._data, payload_class_ptr).contents

        # sanity check on the length
        # the payload length must be at least the header size
        # check the decompressed size is valid
        counts_array_len = len_data - payload_header_size
        if counts_array_len < 0 or counts_array_len % word_size:
            raise HdrLengthException()

        # point directly to the decompressed counts array (no copy)
        self.payload = payload
        self.counts_len = counts_array_len / word_size
        self.word_size = word_size
        return self

    def compress(self, encode_counters_count=-1):
        '''Compress this payload instance
        Args:
            encode_counters_count how many counters should be encoded
                           starting from index 0 (can be 0), if negative will
                           encode the entire array
        Return:
            the compressed payload (python string)
        '''
        if self.payload:
            if encode_counters_count < 0:
                encode_counters_count = self.counts_len
            payload_len = payload_header_size + encode_counters_count * self.word_size
            self.payload.payload_len = payload_len
            data = ctypes.string_at(ctypes.byref(self.payload), payload_len)
            cdata = zlib.compress(data)
            return cdata
        # can't compress if no payload
        raise RuntimeError('No payload to compress')

    def dump(self, label=None):
        if label:
            print 'Payload Dump ' + label
        print '   payload cookie: %x' % (self.payload.cookie)
        print '   payload_len: %d' % (self.payload.payload_len)
        print '   counts_len: %d' % (self.counts_len)
        dump_payload(self.payload.counts, self.counts_len)

class HdrHistogramEncoder(object):
    '''An encoder class for histograms, only supports V1 encoding.
    '''
    def __init__(self, histogram, b64_wrap=True, hdr_payload=None):
        '''Histogram encoder
        Args:
            histogram the histogram to encode/decode into
            b64_wrap determines if the base64 wrapper is enabled or not
            hdr_payload if None will create a new HdrPayload instance for this
                encoder, else will reuse the passed Hdrayload instance (useful
                after decoding one and to associate it to a new histogram)
        '''
        self.histogram = histogram
        if not hdr_payload:
            self.payload = HdrPayload(8, histogram.counts_len)
            payload = self.payload.payload
            # those values never change across encodings
            payload.normalizing_index_offset = 0
            payload.conversion_ratio_bits = 1
            payload.significant_figures = histogram.significant_figures
            payload.lowest_trackable_value = histogram.lowest_trackable_value
            payload.highest_trackable_value = histogram.highest_trackable_value
        else:
            self.payload = hdr_payload

        self.b64_wrap = b64_wrap
        self.header = HdrHeader()
        self.header.cookie = get_compression_cookie(self.payload.word_size)

    def get_counts(self):
        '''Retrieve the counts array that can be used to store live counters
        and that can be encoded with minimal copies using encode()
        '''
        return self.payload.get_counts()

    def encode(self):
        '''Compress the associated encodable payload,
        prepend the header then encode with base64 if requested

        Returns:
            the b64 encoded wire encoding of the histogram (as a string)
            or the compressed payload (as a string, if b64 wrappinb is disabled)
        '''
        # only compress the first non zero buckets
        # if historam is empty we do not encode any counter
        if self.histogram.total_count:
            relevant_length = \
                self.histogram.get_counts_array_index(self.histogram.max_value) + 1
        else:
            relevant_length = 0
        cpayload = self.payload.compress(relevant_length)
        if self.b64_wrap:
            self.header.length = len(cpayload)

            header_str = ctypes.string_at(ctypes.byref(self.header), sizeof(self.header))
            return base64.b64encode(''.join([header_str, cpayload]))
        return cpayload

    @staticmethod
    def decode(encoded_histogram, b64_wrap=True):
        '''Decode a wire histogram encoding into an Hdr Payload instance
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
        if b64_wrap:
            b64decode = base64.b64decode(encoded_histogram)
            # this string has 2 parts in it: the header (raw) and the payload (compressed)

            # use typecast to point to the start of the decode string with an HdrHeader pointer
            header = ctypes.cast(b64decode, ctypes.POINTER(HdrHeader)).contents
            if get_cookie_base(header.cookie) != V1_COMPRESSION_COOKIE_BASE:
                raise HdrCookieException()
            if header.length != len(b64decode) - sizeof(header):
                raise HdrLengthException()
            # this will result in a copy of the compressed payload part
            # could not find a way to do otherwise since zlib.decompress()
            # expects a string (and does not like a buffer or a memoryview object)
            cpayload = b64decode[sizeof(HdrHeader):]
        else:
            cpayload = encoded_histogram
        hdr_payload = HdrPayload().decompress(cpayload)
        return hdr_payload

    def decode_and_add(self, encoded_histogram):
        hdr_payload = HdrHistogramEncoder.decode(encoded_histogram,
                                                 self.histogram.b64_wrap)
        # sanity check the fields
        # in this version we only support same version decode
        hist = self.histogram
        payload = hdr_payload.payload
        if hist.significant_figures != payload.significant_figures or \
           hist.lowest_trackable_value != payload.lowest_trackable_value or \
           hist.highest_trackable_value != payload.highest_trackable_value:
            raise HdrHistogramSettingsException()
        self.histogram.add_counts(hdr_payload.get_counts(), hdr_payload.counts_len)

def _dump_series(start, stop, count):
    if stop <= start + 1:
        # single index range
        print '[%06d] %d' % (start, count)
    else:
        print '[%06d] %d (%d identical)' % (start, count, stop - start)

def dump_payload(counts, max_index):
    print 'counts array size = %d entries' % (max_index)
    if not max_index:
        return
    series_start_index = 0
    current_series_count = counts[0]
    index = 0
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
