'''
A pure python version of the hdr_histogram code

Ported from
https://github.com/HdrHistogram/HdrHistogram (Java)
https://github.com/HdrHistogram/HdrHistogram_c (C)

Coding / Decoding of histograms using the
HdrHistogram V1 compressed transportable format

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
from __future__ import print_function
from builtins import str
from builtins import range
# from builtins import object

import base64
import ctypes
from ctypes import BigEndianStructure
from ctypes import addressof
from ctypes import c_byte
from ctypes import c_ushort
from ctypes import c_uint
from ctypes import c_ulonglong
from ctypes import c_double

import zlib

from pyhdrh import add_array
from pyhdrh import decode
from pyhdrh import encode

V2_ENCODING_COOKIE_BASE = 0x1c849303
V2_COMPRESSION_COOKIE_BASE = 0x1c849304

# LEB128 + ZigZag require up to 9 bytes per word
V2_MAX_WORD_SIZE_IN_BYTES = 9

# allow at most 4 MB counter array size
# that represents 500,000 8 byte counters
MAX_COUNTS_SIZE = 4 * 1024 * 1024

def get_cookie_base(cookie):
    return cookie & ~0xf0

def get_word_size_in_bytes_from_cookie(cookie):
    if (get_cookie_base(cookie) == V2_ENCODING_COOKIE_BASE) or \
       (get_cookie_base(cookie) == V2_COMPRESSION_COOKIE_BASE):
        return V2_MAX_WORD_SIZE_IN_BYTES
    return (cookie & 0xf0) >> 4

def get_encoding_cookie():
    # LSBit of wordsize byte indicates TLZE Encoding
    return V2_ENCODING_COOKIE_BASE | 0x10

def get_compression_cookie():
    # LSBit of wordsize byte indicates TLZE Encoding
    return V2_COMPRESSION_COOKIE_BASE | 0x10

class HdrCookieException(Exception):
    pass

class HdrLengthException(Exception):
    pass

class HdrHistogramSettingsException(Exception):
    pass

# External encoding header
class ExternalHeader(BigEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("cookie", c_uint),
        ("length", c_uint)]


ext_header_size = ctypes.sizeof(ExternalHeader)

# Header for the zlib compressed part
class PayloadHeader(BigEndianStructure):
    _pack_ = 1
    _fields_ = [
        ("cookie", c_uint),
        ("payload_len", c_uint),
        ("normalizing_index_offset", c_uint),
        ("significant_figures", c_uint),
        ("lowest_trackable_value", c_ulonglong),
        ("highest_trackable_value", c_ulonglong),
        ("conversion_ratio_bits", c_double)]


payload_header_size = ctypes.sizeof(PayloadHeader)

# list of supported payload counter ctypes, indexed by the word size
payload_counter_ctype = [None, None,
                         c_ushort,      # index 2
                         None,
                         c_uint,        # index 4
                         None, None, None,
                         c_ulonglong]   # index 8

class HdrPayload():
    '''A class that wraps the ctypes big endian struct that will hold the
    histogram wire format content (including the counters).
    '''
    def __init__(self, word_size, counts_len=0, compressed_payload=None):
        '''Two ways to use this class:
        - for an empty histogram (pass counts_len>0 and compressed_payload=None)
        - for a decoded histogram (counts_len=0 and compressed_payload!=None)

        Params:
            word_size counter size in bytes (2,4,8 byte counters are supported)
            counts_len number of counters to allocate
                ignored if a compressed payload is provided (not None)

            compressed_payload (string) a payload in zlib compressed form,
                decompress and decode the payload header.
                Caller must then invoke init_counts to pass in counts_len so that the
                counts array can be updated from the decoded varint buffer
                None if no compressed payload is to be associated to this instance
        '''
        self.word_size = word_size
        self.counts_len = counts_len
        self._data = None
        try:
            # ctype counter type
            self.counter_ctype = payload_counter_ctype[word_size]
        except IndexError:
            raise ValueError('Invalid word size')
        if not self.counter_ctype:
            raise ValueError('Invalid word size')
        if compressed_payload:
            self._decompress(compressed_payload)
        elif counts_len:
            self.payload = PayloadHeader()
            self.payload.cookie = get_encoding_cookie()
            self._init_counts()
        else:
            raise RuntimeError('counts_len cannot be zero')

    def _init_counts(self):
        self.counts = (self.counter_ctype * self.counts_len)()

    def init_counts(self, counts_len):
        '''Called after instantiating with a compressed payload
        Params:
            counts_len counts size to use based on decoded settings in the header
        '''
        assert self._data and counts_len and self.counts_len == 0
        self.counts_len = counts_len
        self._init_counts()

        results = decode(self._data, payload_header_size, addressof(self.counts),
                         counts_len, self.word_size)
        # no longer needed
        self._data = None
        return results

    def get_counts(self):
        return self.counts

    def _decompress(self, compressed_payload):
        '''Decompress a compressed payload into this payload wrapper.
        Note that the decompressed buffer is saved in self._data and the
        counts array is not yet allocated.

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
        # make sure this instance is pristine
        if self._data:
            raise RuntimeError('Cannot decompress to an instance with payload')
        # Here it is important to keep a reference to the decompressed
        # string so that it does not get garbage collected
        self._data = zlib.decompress(compressed_payload)
        len_data = len(self._data)

        counts_size = len_data - payload_header_size
        if payload_header_size > counts_size > MAX_COUNTS_SIZE:
            raise HdrLengthException('Invalid size:' + str(len_data))

        # copy the first bytes for the header
        self.payload = PayloadHeader.from_buffer_copy(self._data)

        cookie = self.payload.cookie
        if get_cookie_base(cookie) != V2_ENCODING_COOKIE_BASE:
            raise HdrCookieException('Invalid cookie: %x' % cookie)
        word_size = get_word_size_in_bytes_from_cookie(cookie)
        if word_size != V2_MAX_WORD_SIZE_IN_BYTES:
            raise HdrCookieException('Invalid V2 cookie: %x' % cookie)

    def compress(self, counts_limit):
        '''Compress this payload instance
        Args:
            counts_limit how many counters should be encoded
                           starting from index 0 (can be 0),
        Return:
            the compressed payload (python string)
        '''
        if self.payload:
            # worst case varint encoded length is when each counter is at the maximum value
            # in this case 1 more byte per counter is needed due to the more bits
            varint_len = counts_limit * (self.word_size + 1)
            # allocate enough space to fit the header and the varint string
            encode_buf = (c_byte * (payload_header_size + varint_len))()

            # encode past the payload header
            varint_len = encode(addressof(self.counts), counts_limit,
                                self.word_size,
                                addressof(encode_buf) + payload_header_size,
                                varint_len)

            # copy the header after updating the varint stream length
            self.payload.payload_len = varint_len
            ctypes.memmove(addressof(encode_buf), addressof(self.payload), payload_header_size)

            cdata = zlib.compress(ctypes.string_at(encode_buf, payload_header_size + varint_len))
            return cdata
        # can't compress if no payload
        raise RuntimeError('No payload to compress')

    def dump(self, label=None):
        if label:
            print('Payload Dump ' + label)
        print('   payload cookie: %x' % (self.payload.cookie))
        print('   payload_len: %d' % (self.payload.payload_len))
        print('   counts_len: %d' % (self.counts_len))
        dump_payload(self.get_counts(), self.counts_len)


class HdrHistogramEncoder():
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
            word_size counters size in bytes (2, 4 or 8)
        Exceptions:
            ValueError if the word_size value is unsupported
        '''
        self.histogram = histogram
        if not hdr_payload:
            self.payload = HdrPayload(histogram.word_size, histogram.counts_len)
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
        self.header = ExternalHeader()
        self.header.cookie = get_compression_cookie()

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
        # if histogram is empty we do not encode any counter
        if self.histogram.total_count:
            relevant_length = \
                self.histogram.get_counts_array_index(self.histogram.max_value) + 1
        else:
            relevant_length = 0
        cpayload = self.payload.compress(relevant_length)
        if self.b64_wrap:
            self.header.length = len(cpayload)
            header_str = ctypes.string_at(addressof(self.header), ext_header_size)
            return base64.b64encode(header_str + cpayload)
        return cpayload

    @staticmethod
    def decode(encoded_histogram, b64_wrap=True):
        '''Decode a wire histogram encoding into a read-only Hdr Payload instance
        Args:
            encoded_histogram a string containing the wire encoding of a histogram
                              such as one returned from encode()
        Returns:
            an hdr_payload instance with all the decoded/uncompressed fields

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
            b64dec_len = len(b64decode)

            if b64dec_len < ext_header_size:
                raise HdrLengthException('Base64 decoded message too short')

            header = ExternalHeader.from_buffer_copy(b64decode)
            if get_cookie_base(header.cookie) != V2_COMPRESSION_COOKIE_BASE:
                raise HdrCookieException()
            if header.length != b64dec_len - ext_header_size:
                raise HdrLengthException('Decoded length=%d buffer length=%d' %
                                         (header.length, b64dec_len - ext_header_size))
            # this will result in a copy of the compressed payload part
            # could not find a way to do otherwise since zlib.decompress()
            # expects a string (and does not like a buffer or a memoryview object)
            cpayload = b64decode[ext_header_size:]
        else:
            cpayload = encoded_histogram
        hdr_payload = HdrPayload(8, compressed_payload=cpayload)
        return hdr_payload

    def add(self, other_encoder):
        add_array(addressof(self.get_counts()),
                  addressof(other_encoder.get_counts()),
                  self.histogram.counts_len,
                  self.histogram.word_size)

def _dump_series(start, stop, count):
    if stop <= start + 1:
        # single index range
        print('[%06d] %d' % (start, count))
    else:
        print('[%06d] %d (%d identical)' % (start, count, stop - start))

def dump_payload(counts, max_index):
    print('counts array size = %d entries' % (max_index))
    if not max_index:
        return
    series_start_index = 0
    total_count = 0
    current_series_count = counts[0]
    index = 0
    for index in range(1, max_index):
        total_count += counts[index]
        if counts[index] != current_series_count:
            # dump the current series
            _dump_series(series_start_index, index, current_series_count)
            # start a new series
            current_series_count = counts[index]
            series_start_index = index
    # there is always a last series to dump
    _dump_series(series_start_index, index, counts[index])
    print('[%06d] --END-- total count=%d' % (index + 1, total_count))

def hex_dump(label, hstr):
    print(label)
    print(':'.join(x.encode('hex') for x in hstr))
