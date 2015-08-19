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

import base64
import ctypes
from ctypes import c_ushort
from ctypes import c_uint
from ctypes import c_ulonglong
import zlib
import numpy as np


# For now we only use 8-byte counters (64-bits)
V1_ENCODING_COOKIE_BASE = 0x1c849301
V1_COMPRESSION_COOKIE_BASE = 0x1c849302

# allow at most 4 MB counter array size
# that represents 500,000 8 byte couters
MAX_COUNTS_SIZE = 4 * 1024 * 1024

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

# V1 encoding header
# The encoded compressed format is as follows:
# base64( HdrHeader + zlibcompress(HdrPayload<bits>) )
hdr_header_dtype = np.dtype([("cookie", ">u4"),
                             ("length", ">u4")])

# Common header fields for the zlib compressed part
# > denotes big endian (>u4 is big endian unsigned 4-byte integer)
#
payload_hdr_fields = [("cookie", ">u4"),
                      ("payload_len", ">u4"),
                      ("normalizing_index_offset", ">u4"),
                      ("significant_figures", ">u4"),
                      ("lowest_trackable_value", ">u8"),
                      ("highest_trackable_value", ">u8"),
                      ("conversion_ratio_bits", ">u8")]

payload_header_dtype = np.dtype(payload_hdr_fields)
payload_header_size = payload_header_dtype.itemsize
# list of supported payload counter types, indexed by the word size
payload_counter_type = [None, None,
                        '>u2',   # index 2
                        None,
                        '>u4',   # index 4
                        None, None, None,
                        '>u8']   # index 8
# same but for ctypes
payload_counter_ctype = [None, None,
                         c_ushort,      # index 2
                         None,
                         c_uint,        # index 4
                         None, None, None,
                         c_ulonglong]   # index 8

def get_hdr_dtype(word_size, counts_len):
    '''Return the numpy dtype for a given word size and array size
    '''
    # we only support 2,4,8 as word sizes
    try:
        counter_type = payload_counter_type[word_size]
    except IndexError:
        raise ValueError('Invalid word size')
    if not counter_type:
        raise ValueError('Invalid word size')
    hdt = np.dtype(payload_hdr_fields + [("counts", (counter_type, (counts_len)))])
    return hdt

def get_hdr_ctypes(word_size, counts_len):
    '''Return the ctypes struct class for a given word size and array size
    '''
    # Need to create a class as it is teh only way to specify big endian items
    class AnyHdrPayload(ctypes.BigEndianStructure):
        _pack_ = 1
        _fields_ = [("counts", payload_counter_ctype[word_size] * counts_len)]
    AnyHdrPayload.__name__ = 'HdrPayload' + str(word_size) + '_' + str(counts_len)
    return AnyHdrPayload

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
        self._data = None
        if word_size:
            # now that we now the word size, typecast with the right class ptr
            self.payload = np.zeros(1, dtype=get_hdr_dtype(word_size, counts_len))[0]
            self.payload['cookie'] = get_encoding_cookie(word_size)
        else:
            self.payload = None
            self.counts_len = 0
        self.counts = None

    def get_counts(self):
        # Numpy array item access is extremely slow due to the overhead of the
        # numpy array access code, often over 10x slower than native ctypes access
        # So to take care of this, we're going to provide a direct access
        # to the numpy counts ndarray using a ctypes array, there is no
        # copy, just 2 ways to access the same counts array, one being much
        # faster
        if not self.counts:
            np_counts = self.payload['counts']
            # define a big endian structure that has only an array of the right
            # counter type
            hdr_ptr = ctypes.POINTER(get_hdr_ctypes(self.word_size, self.counts_len))
            self.counts = np_counts.ctypes.data_as(hdr_ptr).contents.counts
        return self.counts

    def get_np_counts(self):
        ''' Return the numpy array - which is much faster when doing array
        operations.
        '''
        return self.payload['counts']

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
        # the counts array can be a multiple of 2, 4 or 8
        # we need an exact length match to get a dtype,
        # multiple of 2 will always work
        counts_size = len_data - payload_header_size
        if counts_size & 1:
            raise HdrLengthException('Decompressed size cannot be odd')
        if payload_header_size > counts_size > MAX_COUNTS_SIZE:
            raise HdrLengthException('Invalid size:' + str(len_data))

        # copy the first bytes for the header
        header = self._data[:payload_header_size]
        #
        # Access the payload header
        payload = np.frombuffer(header, dtype=payload_header_dtype)[0]

        cookie = payload['cookie']
        if get_cookie_base(cookie) != V1_ENCODING_COOKIE_BASE:
            raise HdrCookieException('Invalid cookie')
        word_size = get_word_size_in_bytes_from_cookie(cookie)

        # check the size is a multiple of the word size
        if counts_size & (word_size - 1):
            raise HdrLengthException('Not a multiple of the word size:' + str(counts_size))
        counts_len = counts_size / word_size

        # now that we now the word size get the exact numpy type for this
        # decoded payload
        payload = np.frombuffer(self._data,
                                dtype=get_hdr_dtype(word_size, counts_len))[0]

        # point directly to the decompressed counts array (no copy)
        self.payload = payload
        self.counts_len = counts_len
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
            elif encode_counters_count > self.counts_len:
                raise IndexError('number of counters to compress too large')
            payload_len = payload_header_size + encode_counters_count * self.word_size
            self.payload['payload_len'] = payload_len
            data_address = self.payload.__array_interface__['data'][0]
            data_str = ctypes.string_at(data_address, payload_len)
            cdata = zlib.compress(data_str)
            return cdata
        # can't compress if no payload
        raise RuntimeError('No payload to compress')

    def dump(self, label=None):
        if label:
            print 'Payload Dump ' + label
        print '   payload cookie: %x' % (self.payload['cookie'])
        print '   payload_len: %d' % (self.payload['payload_len'])
        print '   counts_len: %d' % (self.counts_len)
        dump_payload(self.get_counts(), self.counts_len)

class HdrHistogramEncoder(object):
    '''An encoder class for histograms, only supports V1 encoding.
    '''
    def __init__(self, histogram, b64_wrap=True, hdr_payload=None, word_size=8):
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
            self.payload = HdrPayload(word_size, histogram.counts_len)
            payload = self.payload.payload
            # those values never change across encodings
            payload['normalizing_index_offset'] = 0
            payload['conversion_ratio_bits'] = 1
            payload['significant_figures'] = histogram.significant_figures
            payload['lowest_trackable_value'] = histogram.lowest_trackable_value
            payload['highest_trackable_value'] = histogram.highest_trackable_value
        else:
            self.payload = hdr_payload
            self.update_counts()

        self.b64_wrap = b64_wrap
        self.header = np.zeros(1, dtype=hdr_header_dtype)[0]
        self.header['cookie'] = get_compression_cookie(self.payload.word_size)

    def get_counts(self):
        '''Retrieve the counts array that can be used to store live counters
        and that can be encoded with minimal copies using encode()
        Note that we return here the ctypes array and not the numpy array
        because array item arithmetics are very slow with numpy array (the
        ctypes and numpy arrays both point to the same memory location)
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
            self.header['length'] = len(cpayload)
            data_address = self.header.__array_interface__['data'][0]
            header_str = ctypes.string_at(data_address, hdr_header_dtype.itemsize)
            return base64.b64encode(''.join([header_str, cpayload]))
        return cpayload

    @staticmethod
    def decode(encoded_histogram, b64_wrap=True):
        '''Decode a wire histogram encoding into a read-only Hdr Payload instance
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
            b64dec_len = len(b64decode)

            if b64dec_len < hdr_header_dtype.itemsize:
                raise HdrLengthException('Base64 decoded message too short')

            hdr_header_str = b64decode[:hdr_header_dtype.itemsize]
            header = np.frombuffer(hdr_header_str, dtype=hdr_header_dtype)[0]
            if get_cookie_base(header['cookie']) != V1_COMPRESSION_COOKIE_BASE:
                raise HdrCookieException()
            if header['length'] != b64dec_len - hdr_header_dtype.itemsize:
                raise HdrLengthException('Decoded length=%d buffer length=%d' %
                                         (header['length'], b64dec_len - hdr_header_dtype.itemsize))
            # this will result in a copy of the compressed payload part
            # could not find a way to do otherwise since zlib.decompress()
            # expects a string (and does not like a buffer or a memoryview object)
            cpayload = b64decode[hdr_header_dtype.itemsize:]
        else:
            cpayload = encoded_histogram
        hdr_payload = HdrPayload().decompress(cpayload)
        return hdr_payload

    def update_counts(self, np_counts=None):
        '''Update the histogram min/max, total count based on a given numpy array

        Params:
            np_counts if None will update the associated histogram min.max/total
                count based on the content of the associated payload
                If not None, will add the passed numpy array to teh current
                histogram and update the min/max/total count
        '''
        if np_counts is None:
            np_counts = self.payload.get_np_counts()
            # prevent adding, just update the min/max/total
            dest_np_counts = None
        else:
            dest_np_counts = self.payload.get_np_counts()
        counts_len = np_counts.size
        if counts_len:
            # check for out of bounds
            if counts_len > self.payload.counts_len:
                raise IndexError('Decoded histogram has too many counters')

            # find out all the indices that have a non zero value
            # a bit over kill for this as we only need to find the first
            # and last non zero indices but this is native C speed so very fast
            nonzeros = np.nonzero(np_counts)[0]
            if nonzeros.size:
                lowest_non_zero_index = nonzeros[0]
                highest_non_zero_index = nonzeros[-1]

                if dest_np_counts is not None:
                    # now the size of the destination array may be larger than the
                    # size of the decoded array because the last entries that are
                    # zeros are not encoded, numpy only supports array additions
                    # of same shape so we need first to get a view of the destination
                    # array that is the same size as the decoded array (there is
                    # no copy so this should be very fast)
                    dest_np_counts = dest_np_counts[:counts_len]

                    # do an in-place addition of one array to another
                    np.add(dest_np_counts, np_counts, dest_np_counts)

                # another very fast operation is to get the sum of all
                # entries in the array
                observed_other_total_count = np_counts.sum()
                self.histogram.adjust_internal_tacking_values(
                    lowest_non_zero_index,
                    highest_non_zero_index,
                    observed_other_total_count)

    def decode_and_add(self, encoded_histogram):
        '''This operation is more efficient than doing a decode into a histogram
        then add the histogram to another one because it bypasses the
        overhead of initializing a new histogram instance.
        '''
        hdr_payload = HdrHistogramEncoder.decode(encoded_histogram,
                                                 self.histogram.b64_wrap)
        # sanity check the fields
        # in this version we only support same version decode
        hist = self.histogram
        payload = hdr_payload.payload
        if hist.significant_figures != payload['significant_figures'] or \
           hist.lowest_trackable_value != payload['lowest_trackable_value'] or \
           hist.highest_trackable_value != payload['highest_trackable_value']:
            raise HdrHistogramSettingsException()
        self.update_counts(hdr_payload.get_np_counts())

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

def hex_dump(label, str):
    print label
    print ':'.join(x.encode('hex') for x in str)
