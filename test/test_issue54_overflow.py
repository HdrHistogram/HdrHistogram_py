'''
Regression tests for issue #54 — `HdrHistogram.add()` and `HdrHistogram.encode()`
crashed with `OverflowError` on 64-bit Windows.

`py_hdr_encode` and `py_hdr_add_array` parsed pointer arguments via the
`PyArg_ParseTuple` "l" format. On 64-bit Windows (LLP64) `sizeof(long) == 4`,
so any heap address above 2**31 - 1 raised `OverflowError` before the C body
ran. The fix switches both call sites to "L" (long long, always 64-bit),
matching what #37 already did for `py_hdr_decode`.

These tests cannot reproduce the original `OverflowError` on LP64 hosts
(Linux, macOS) because there `long` is already 64-bit, but they:
  - assert the C functions accept addresses that overflow a 32-bit `long`
    (the precise property that was broken on Win64);
  - lock in the behaviour of the public `add()` / `encode()` paths that
    triggered the crash;
  - guard against accidental regression of the format spec back to "l".

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
'''
from ctypes import addressof
from ctypes import c_uint8
from ctypes import c_uint64
from ctypes import sizeof

import pytest

from pyhdrh import add_array  # pylint: disable=no-name-in-module,import-error
from pyhdrh import encode     # pylint: disable=no-name-in-module,import-error

from hdrh.histogram import HdrHistogram


# Address far above 2**31 - 1 (the upper bound of a signed 32-bit `long`).
# 2**40 is comfortably inside `long long` range and outside `long` on Windows.
ABOVE_LONG_MAX_ADDR = 1 << 40

# Anything above 2**63 - 1 must still raise OverflowError ("L" is signed).
ABOVE_LONG_LONG_MAX = 1 << 65

ARRAY_SIZE = 10


@pytest.mark.pyhdrh
def test_encode_accepts_64bit_address():
    """`encode` must accept addresses that overflow a 32-bit `long`.

    With max_index=0 the C function returns 0 before dereferencing, so we
    can probe argument parsing without touching memory. Before the fix this
    raised OverflowError on Win64.
    """
    res = encode(ABOVE_LONG_MAX_ADDR, 0, sizeof(c_uint64),
                 ABOVE_LONG_MAX_ADDR, 0)
    assert res == 0


@pytest.mark.pyhdrh
def test_add_array_accepts_64bit_address():
    """`add_array` must accept addresses that overflow a 32-bit `long`.

    With max_index=0 the C function never dereferences the pointers, so we
    can verify argument parsing accepts a 64-bit address without UB.
    """
    res = add_array(ABOVE_LONG_MAX_ADDR, ABOVE_LONG_MAX_ADDR, 0,
                    sizeof(c_uint64))
    assert res == 0


@pytest.mark.pyhdrh
def test_encode_rejects_above_long_long():
    """Values that don't fit in `long long` must still raise OverflowError."""
    with pytest.raises(OverflowError):
        encode(ABOVE_LONG_LONG_MAX, 0, sizeof(c_uint64), 0, 0)


@pytest.mark.pyhdrh
def test_add_array_rejects_above_long_long():
    """Values that don't fit in `long long` must still raise OverflowError."""
    with pytest.raises(OverflowError):
        add_array(ABOVE_LONG_LONG_MAX, ABOVE_LONG_LONG_MAX, 0,
                  sizeof(c_uint64))


@pytest.mark.pyhdrh
def test_encode_real_buffer_high_address():
    """`encode` must work against ctypes buffers regardless of heap address.

    On modern 64-bit OSes the heap routinely lives above 2**31, so this
    exercises the same code path that crashed on Win64.
    """
    src_array = (c_uint64 * ARRAY_SIZE)()
    dst_len = 9 * ARRAY_SIZE
    dst_array = (c_uint8 * dst_len)()
    src_addr = addressof(src_array)
    dst_addr = addressof(dst_array)
    src_array[ARRAY_SIZE - 1] = 1
    res = encode(src_addr, ARRAY_SIZE, sizeof(c_uint64), dst_addr, dst_len)
    # 9 zeros => -9 zigzag = 0x11; final 1 => zigzag 0x02
    assert res == 2
    assert dst_array[0] == 0x11
    assert dst_array[1] == 0x02


@pytest.mark.pyhdrh
def test_add_array_real_buffer_high_address():
    """`add_array` must work against ctypes buffers regardless of heap address."""
    src_array = (c_uint64 * ARRAY_SIZE)()
    dst_array = (c_uint64 * ARRAY_SIZE)()
    src_addr = addressof(src_array)
    dst_addr = addressof(dst_array)
    expected_total = 0
    for index in range(ARRAY_SIZE):
        src_array[index] = index + 1
        dst_array[index] = 1
        expected_total += index + 1
    added = add_array(dst_addr, src_addr, ARRAY_SIZE, sizeof(c_uint64))
    assert added == expected_total
    for index in range(ARRAY_SIZE):
        assert dst_array[index] == (index + 1) + 1


@pytest.mark.codec
@pytest.mark.parametrize("word_size", [2, 4, 8])
def test_histogram_add_round_trip(word_size):
    """Public-API regression for the `add()` crash reported in issue #54.

    Reproduces the scenario from the bug report end-to-end: build two
    histograms, call `add()` (which internally calls `add_array` against
    ctypes addresses), then verify the merged counts are correct. Runs
    against every supported counter word size.
    """
    hist_a = HdrHistogram(1, 3_600_000_000, 3, word_size=word_size)
    hist_b = HdrHistogram(1, 3_600_000_000, 3, word_size=word_size)
    hist_a.record_value(1000)
    hist_a.record_value(2_000_000)
    hist_b.record_value(2000)
    hist_b.record_value(2_000_000)

    hist_a.add(hist_b)

    assert hist_a.get_total_count() == 4
    assert hist_a.get_count_at_value(1000) == 1
    assert hist_a.get_count_at_value(2000) == 1
    assert hist_a.get_count_at_value(2_000_000) == 2


@pytest.mark.codec
@pytest.mark.parametrize("word_size", [2, 4, 8])
def test_histogram_encode_round_trip(word_size):
    """Public-API regression for the `encode()` crash from issue #54.

    `encode()` ultimately calls `pyhdrh.encode` with two ctypes addresses
    (the source counts array and the destination buffer). Both must accept
    a 64-bit address. After the fix this round-trip works on every
    platform; before the fix it crashed on Win64 with OverflowError.
    """
    hist = HdrHistogram(1, 3_600_000_000, 3, word_size=word_size)
    for value in (1000, 25_000, 999_999, 1_500_000_000):
        hist.record_value(value)
    blob = hist.encode()
    assert blob

    decoded = HdrHistogram.decode(blob)
    assert decoded.get_total_count() == hist.get_total_count()
    for value in (1000, 25_000, 999_999, 1_500_000_000):
        assert decoded.get_count_at_value(value) == hist.get_count_at_value(value)


@pytest.mark.codec
def test_decode_and_add_chain():
    """End-to-end check: encode -> decode -> add chain must not raise."""
    hist = HdrHistogram(1, 3_600_000_000, 3)
    hist.record_value(42)
    hist.record_value(424_242)
    blob = hist.encode()

    accumulator = HdrHistogram(1, 3_600_000_000, 3)
    accumulator.decode_and_add(blob)
    accumulator.decode_and_add(blob)
    assert accumulator.get_total_count() == 4
    assert accumulator.get_count_at_value(42) == 2
    assert accumulator.get_count_at_value(424_242) == 2
