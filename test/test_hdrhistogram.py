'''
Most of the test code is inspired from the test code in:
https://github.com/HdrHistogram/HdrHistogram_c.git
and converted to python


'''
from hdrh.histogram import HdrHistogram

# histogram __init__ values
LOWEST = 1
HIGHEST = 3600 * 1000 * 1000
SIGNIFICANT = 3
TEST_VALUE_LEVEL = 4

def test_basic():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert(histogram.bucket_count == 22)
    assert(histogram.sub_bucket_count == 2048)
    assert(histogram.counts_len == 23552)
    assert(histogram.unit_magnitude == 0)
    assert(histogram.sub_bucket_half_count_magnitude == 10)

def test_large_numbers():
    histogram = HdrHistogram(20000000, 100000000, 5)
    histogram.record_value(100000000)
    histogram.record_value(20000000)
    histogram.record_value(30000000)
    assert(histogram.values_are_equivalent(20000000, histogram.get_value_at_percentile(50.0)))
    assert(histogram.values_are_equivalent(30000000, histogram.get_value_at_percentile(83.33)))
    assert(histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(83.34)))
    assert(histogram.values_are_equivalent(100000000, histogram.get_value_at_percentile(99.0)))

def test_record_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    histogram.record_value(TEST_VALUE_LEVEL)
    assert(histogram.get_count_at_value(TEST_VALUE_LEVEL) == 1)
    assert(histogram.get_total_count() == 1)

def test_highest_equivalent_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert 8183 * 1024 + 1023 == histogram.get_highest_equivalent_value(8180 * 1024)
    assert 8191 * 1024 + 1023 == histogram.get_highest_equivalent_value(8191 * 1024)
    assert 8199 * 1024 + 1023 == histogram.get_highest_equivalent_value(8193 * 1024)
    assert 9999 * 1024 + 1023 == histogram.get_highest_equivalent_value(9995 * 1024)
    assert 10007 * 1024 + 1023 == histogram.get_highest_equivalent_value(10007 * 1024)
    assert 10015 * 1024 + 1023 == histogram.get_highest_equivalent_value(10008 * 1024)

def test_scaled_highest_equiv_value():
    histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    assert 8183 == histogram.get_highest_equivalent_value(8180)
    assert 8191 == histogram.get_highest_equivalent_value(8191)
    assert 8199 == histogram.get_highest_equivalent_value(8193)
    assert 9999 == histogram.get_highest_equivalent_value(9995)
    assert 10007 == histogram.get_highest_equivalent_value(10007)
    assert 10015 == histogram.get_highest_equivalent_value(10008)

def load_histogram():

    raw_histogram = HdrHistogram(LOWEST, HIGHEST, SIGNIFICANT)
    # record this value with a count of 10,000
    raw_histogram.record_value(1000L, 10000)

    raw_histogram.record_value(100000000L)
    return raw_histogram

def check_percentile(hist, percentile, value, variation):
    value_at = hist.get_value_at_percentile(percentile)
    assert(abs(value_at - value) < value * variation)

def test_percentiles():
    hist = load_histogram()
    check_percentile(hist, 30.0, 1000.0, 0.001)
    check_percentile(hist, 99.0, 1000.0, 0.001)
    check_percentile(hist, 99.99, 1000.0, 0.001)
    check_percentile(hist, 99.999, 100000000.0, 0.001)
    check_percentile(hist, 100.0, 100000000.0, 0.001)
    assert(hist.get_total_count() == 10001)
    assert(hist.values_are_equivalent(hist.get_min_value(), 1000.0))
    assert(hist.values_are_equivalent(hist.get_max_value(), 100000000.0))

def test_recorded_iterator():

    hist = load_histogram()
    itr = iter(hist)
    index = 0
    for _ in itr:
        count_added_in_this_bucket = itr.count_at_index
        if index == 0:
            assert(count_added_in_this_bucket == 10000)
        else:
            assert(count_added_in_this_bucket == 1)
        index += 1
    assert(index == 2)

def check_iterator_values(itr, last_index):
    index = 0
    for _ in itr:
        count_added_in_this_bucket = itr.count_added_in_this_iter_step
        if index == 0:
            assert(count_added_in_this_bucket == 10000)
        elif index == last_index:
            assert(count_added_in_this_bucket == 1)
        else:
            assert(count_added_in_this_bucket == 0)
        index += 1
    assert(index - 1 == last_index)

def test_linear_iterator():
    hist = load_histogram()
    itr = hist.get_linear_iterator(100000)
    check_iterator_values(itr, 999)

def test_log_iterator():
    hist = load_histogram()
    itr = hist.get_log_iterator(10000, 2.0)
    check_iterator_values(itr, 14)

def test_percentile_iterator():
    hist = load_histogram()
    # test with 5 ticks per half distance
    itr = hist.get_percentile_iterator(5)
    for value in itr:
        expected = hist.get_highest_equivalent_value(hist.get_value_at_percentile(itr.percentile))
        assert(value == expected)

# These data are generated from an actual wrk2 run (wrk2 uses hdr_histogram.c),
# to be used as a reference
# that the python implementation of HdrHistogram must match for percentile
# values when latency buckets are imported
# Wrk2 uses usec units from 1 to 24 hours
IMPORTED_MAX_LATENCY = 24 * 60 * 60 * 1000000
IMPORTED_LATENCY_DATA0 = {
    "buckets": 27, "sub_buckets": 2048, "digits": 3,
    "max_latency": IMPORTED_MAX_LATENCY,
    "min": 89151,
    "max": 209664,
    "counters": [
        6, [1295, 1, 1392, 1, 1432, 1, 1435, 1, 1489, 1, 1493, 1, 1536, 1, 1553, 1,
            1560, 1, 1574, 1, 1591, 1, 1615, 1, 1672, 1, 1706, 1, 1738, 1, 1812, 1,
            1896, 1],
        7, [1559, 1, 1590, 1, 1638, 1]],

    # pairs of value in ms and percentile
    # this is only present/used for UT to verify that the output is correct
    "value_percentile":
        [82.943, 0.000000,
         89.151, 0.100000,
         91.903, 0.200000,
         95.615, 0.300000,
         99.455, 0.400000,
         100.799, 0.500000,
         101.887, 0.550000,
         103.423, 0.600000,
         107.071, 0.650000,
         109.247, 0.700000,
         111.295, 0.750000,
         116.031, 0.775000,
         116.031, 0.800000,
         121.407, 0.825000,
         121.407, 0.850000,
         199.679, 0.875000,
         199.679, 0.887500,
         199.679, 0.900000,
         199.679, 0.912500,
         203.647, 0.925000,
         203.647, 0.937500,
         203.647, 0.943750,
         203.647, 0.950000,
         203.647, 0.956250,
         209.791, 1.000000]
}

# Another run capture on a slower URL and with longer capture window
IMPORTED_LATENCY_DATA1 = {
    "buckets": 27, "sub_buckets": 2048, "digits": 3,
    "max_latency": IMPORTED_MAX_LATENCY,
    "min": 4931583,
    "max": 15384576,
    "counters":
    # list of bucket_index, [sub_bucket_index, count...]
    [12, [1203, 1, 1272, 1, 1277, 1, 1278, 1, 1296, 1, 1300, 1, 1306, 1, 1316, 1,
          1320, 1, 1323, 1, 1325, 1, 1334, 3, 1336, 1, 1338, 1, 1342, 1, 1346, 1,
          1348, 1, 1365, 1, 1366, 1, 1373, 1, 1399, 1, 1400, 1, 1403, 1, 1405, 2,
          1408, 1, 1410, 1, 1420, 3, 1426, 1, 1428, 1, 1434, 1, 1439, 1, 1450, 1,
          1456, 1, 1461, 1, 1463, 1, 1470, 1, 1475, 1, 1476, 1, 1484, 1, 1487, 1,
          1489, 2, 1504, 1, 1505, 1, 1507, 2, 1510, 1, 1515, 1, 1529, 2, 1530, 2,
          1539, 1, 1545, 1, 1551, 2, 1561, 1, 1562, 1, 1563, 1, 1568, 1, 1584, 1,
          1585, 1, 1599, 1, 1600, 2, 1610, 1, 1612, 1, 1614, 1, 1619, 1, 1623, 1,
          1626, 2, 1635, 1, 1638, 1, 1650, 1, 1652, 1, 1653, 1, 1657, 1, 1666, 1,
          1675, 2, 1677, 1, 1680, 1, 1682, 1, 1688, 1, 1692, 2, 1694, 1, 1706, 1,
          1709, 1, 1710, 1, 1717, 2, 1722, 1, 1728, 1, 1730, 1, 1731, 1, 1734, 1,
          1736, 1, 1745, 1, 1753, 1, 1757, 1, 1761, 1, 1762, 1, 1766, 2, 1768, 2,
          1776, 1, 1792, 1, 1794, 1, 1796, 1, 1799, 1, 1804, 1, 1810, 1, 1819, 1,
          1826, 1, 1828, 1, 1834, 1, 1835, 1, 1837, 1, 1842, 1, 1843, 1, 1844, 1,
          1845, 1, 1857, 1, 1864, 1, 1870, 1, 1876, 1, 1877, 2, 1880, 1, 1889, 1,
          1893, 1, 1895, 1, 1912, 1, 1913, 1, 1914, 1, 1924, 1, 1927, 1, 1930, 1,
          1939, 1, 1940, 1, 1941, 2, 1945, 1, 1949, 1, 1950, 1, 1951, 1, 1954, 1,
          1955, 1, 1957, 1, 1965, 1, 1967, 1, 1969, 1, 1977, 1, 1983, 1, 1987, 1,
          1991, 1, 1992, 1, 2003, 1, 2009, 1, 2014, 1, 2019, 1, 2022, 1, 2024, 1,
          2026, 1, 2035, 1, 2040, 1, 2047, 2],
     13, [1024, 1, 1027, 2, 1030, 1, 1031, 1,
          1032, 1, 1033, 1, 1039, 2, 1042, 1, 1044, 1, 1050, 1, 1052, 1, 1053, 1,
          1054, 1, 1055, 1, 1057, 1, 1058, 1, 1059, 1, 1062, 1, 1063, 1, 1065, 1,
          1068, 2, 1071, 1, 1073, 1, 1076, 1, 1077, 1, 1080, 1, 1082, 1, 1083, 1,
          1085, 1, 1088, 1, 1091, 2, 1098, 1, 1103, 1, 1105, 1, 1110, 1, 1113, 2,
          1114, 2, 1116, 1, 1122, 1, 1123, 2, 1124, 1, 1126, 1, 1128, 1, 1129, 1,
          1130, 1, 1138, 1, 1141, 1, 1143, 1, 1144, 1, 1146, 1, 1149, 1, 1151, 1,
          1157, 2, 1160, 1, 1164, 1, 1165, 3, 1168, 1, 1169, 1, 1171, 1, 1174, 1,
          1180, 1, 1184, 1, 1188, 1, 1195, 1, 1197, 1, 1198, 1, 1201, 1, 1202, 1,
          1205, 2, 1209, 1, 1210, 1, 1212, 1, 1213, 1, 1214, 1, 1215, 1, 1216, 2,
          1220, 1, 1222, 1, 1225, 1, 1226, 1, 1229, 1, 1233, 1, 1238, 2, 1241, 1,
          1242, 2, 1243, 1, 1245, 1, 1246, 1, 1247, 1, 1253, 2, 1259, 1, 1261, 1,
          1262, 1, 1266, 1, 1268, 2, 1269, 2, 1270, 1, 1271, 1, 1272, 1, 1273, 1,
          1278, 1, 1280, 1, 1282, 1, 1283, 1, 1289, 1, 1292, 1, 1296, 1, 1297, 1,
          1299, 1, 1304, 1, 1305, 2, 1306, 2, 1312, 1, 1314, 1, 1316, 1, 1318, 1,
          1320, 1, 1324, 1, 1325, 1, 1327, 2, 1328, 1, 1332, 1, 1334, 1, 1335, 2,
          1338, 2, 1344, 1, 1347, 1, 1348, 1, 1349, 1, 1351, 1, 1354, 1, 1355, 1,
          1359, 1, 1362, 1, 1363, 2, 1364, 1, 1366, 1, 1368, 1, 1374, 1, 1385, 2,
          1389, 1, 1390, 2, 1393, 1, 1394, 1, 1396, 1, 1397, 1, 1399, 1, 1400, 1,
          1403, 2, 1410, 1, 1413, 1, 1417, 1, 1419, 1, 1420, 1, 1421, 1, 1422, 1,
          1426, 1, 1432, 1, 1433, 1, 1434, 1, 1435, 2, 1438, 1, 1439, 1, 1443, 2,
          1446, 1, 1448, 1, 1459, 1, 1461, 1, 1467, 3, 1471, 1, 1474, 1, 1475, 1,
          1477, 1, 1479, 1, 1480, 1, 1484, 2, 1487, 2, 1496, 1, 1499, 1, 1501, 2,
          1503, 1, 1507, 2, 1508, 1, 1509, 1, 1514, 1, 1519, 2, 1520, 1, 1522, 1,
          1537, 1, 1538, 1, 1539, 1, 1541, 1, 1544, 1, 1545, 2, 1548, 1, 1552, 1,
          1554, 1, 1557, 1, 1561, 1, 1566, 1, 1567, 1, 1569, 2, 1575, 1, 1578, 3,
          1580, 1, 1581, 1, 1582, 1, 1586, 2, 1596, 1, 1597, 2, 1598, 1, 1601, 1,
          1603, 1, 1609, 1, 1611, 1, 1616, 2, 1619, 1, 1621, 1, 1622, 1, 1623, 1,
          1624, 1, 1631, 1, 1632, 1, 1634, 1, 1635, 1, 1637, 2, 1642, 1, 1644, 1,
          1646, 1, 1654, 1, 1655, 1, 1660, 1, 1661, 1, 1664, 1, 1665, 1, 1667, 1,
          1671, 1, 1674, 1, 1675, 1, 1677, 1, 1679, 1, 1681, 1, 1684, 1, 1686, 2,
          1687, 1, 1689, 1, 1690, 1, 1691, 1, 1694, 1, 1695, 1, 1697, 1, 1701, 1,
          1704, 1, 1707, 1, 1711, 1, 1712, 1, 1716, 3, 1719, 1, 1720, 1, 1721, 2,
          1730, 1, 1731, 2, 1738, 1, 1741, 1, 1743, 2, 1744, 3, 1745, 1, 1752, 1,
          1754, 1, 1758, 1, 1760, 1, 1761, 1, 1764, 1, 1765, 1, 1766, 1, 1773, 1,
          1782, 1, 1785, 1, 1786, 1, 1790, 1, 1792, 1, 1795, 1, 1802, 1, 1806, 1,
          1813, 2, 1826, 1, 1827, 1, 1830, 1, 1841, 1, 1852, 1, 1878, 1]],
    "value_percentile":
        [4931.583, 0.000000,
         6189.055, 0.100000,
         7114.751, 0.200000,
         8011.775, 0.300000,
         8896.511, 0.400000,
         9945.087, 0.500000,
         10379.263, 0.550000,
         10805.247, 0.600000,
         11214.847, 0.650000,
         11763.711, 0.700000,
         12304.383, 0.750000,
         12615.679, 0.775000,
         12861.439, 0.800000,
         13099.007, 0.825000,
         13377.535, 0.850000,
         13647.871, 0.875000,
         13778.943, 0.887500,
         13852.671, 0.900000,
         13991.935, 0.912500,
         14090.239, 0.925000,
         14245.887, 0.937500,
         14286.847, 0.943750,
         14295.039, 0.950000,
         14376.959, 0.956250,
         14458.879, 0.962500,
         14532.607, 0.968750,
         14606.335, 0.971875,
         14639.103, 0.975000,
         14688.255, 0.978125,
         14712.831, 0.981250,
         14802.943, 0.984375,
         14860.287, 0.985938,
         14860.287, 0.987500,
         14860.287, 0.989062,
         14966.783, 0.990625,
         14974.975, 0.992188,
         14974.975, 0.992969,
         14999.551, 0.993750,
         14999.551, 0.994531,
         15089.663, 0.995313,
         15089.663, 0.996094,
         15089.663, 0.996484,
         15089.663, 0.996875,
         15179.775, 0.997266,
         15179.775, 0.997656,
         15179.775, 0.998047,
         15179.775, 0.998242,
         15179.775, 0.998437,
         15179.775, 0.998633,
         15179.775, 0.998828,
         15179.775, 0.999023,
         15392.767, 0.999121,
         15392.767, 1.000000]
}

def check_imported_buckets(latency_data):
    histogram = HdrHistogram(LOWEST, IMPORTED_MAX_LATENCY, SIGNIFICANT)
    assert(histogram.add_bucket_counts(latency_data))
    value_at_percentile = latency_data['value_percentile']
    for index in range(0, len(value_at_percentile), 2):
        expected_value = value_at_percentile[index]
        percentile = value_at_percentile[index + 1] * 100
        value = float(histogram.get_value_at_percentile(percentile)) / 1000
        # print '%f%% %f exp:%f' % (percentile, value, expected_value)
        assert(value == expected_value)
    # check min and max
    assert(histogram.values_are_equivalent(histogram.get_min_value(), latency_data['min']))
    assert(histogram.values_are_equivalent(histogram.get_max_value(), latency_data['max']))

def test_imported_buckets():
    check_imported_buckets(IMPORTED_LATENCY_DATA0)
    check_imported_buckets(IMPORTED_LATENCY_DATA1)

def test_percentile_list():
    histogram = HdrHistogram(LOWEST, IMPORTED_MAX_LATENCY, SIGNIFICANT)
    assert(histogram.add_bucket_counts(IMPORTED_LATENCY_DATA0))
    value_at_percentile = IMPORTED_LATENCY_DATA0['value_percentile']
    perc_list = value_at_percentile[1::2]
    expected_value_list = value_at_percentile[0::2]
    perc_dict = histogram.get_percentile_to_value_dict([x * 100 for x in perc_list])
    for perc, exp_value in zip(perc_list, expected_value_list):
        assert(float(perc_dict[perc * 100]) / 1000 == exp_value)

IMPORTED_LATENCY_DATA3 = {
    "buckets": 27, "sub_buckets": 2048, "digits": 3,
    "max_latency": IMPORTED_MAX_LATENCY,
    "min": 89151,
    "max": 209664,
    "counters": [
        6, [1295, 1, 1392, 1, 1432, 1, 1435, 1, 1489, 1, 1493, 1, 1536, 1, 1553, 1,
            1560, 1, 1574, 1, 1591, 1, 1615, 1, 1672, 1, 1706, 1, 1738, 1, 1812, 1,
            1896, 1],
        7, [1559, 1, 1590, 1, 1638, 1]]
}

def test_add_imported_buckets():
    histogram = HdrHistogram(LOWEST, IMPORTED_MAX_LATENCY, SIGNIFICANT)
    latency_data = IMPORTED_LATENCY_DATA3
    assert(histogram.add_bucket_counts(latency_data))
    total = histogram.get_total_count()
    # remove the optional keys
    del latency_data['buckets']
    del latency_data['sub_buckets']
    del latency_data['digits']
    del latency_data['max_latency']
    assert(histogram.add_bucket_counts(latency_data))

    # check count is double
    assert(histogram.get_total_count() == 2 * total)

    # check min and max have not changed
    assert(histogram.values_are_equivalent(histogram.get_min_value(), latency_data['min']))
    assert(histogram.values_are_equivalent(histogram.get_max_value(), latency_data['max']))

    # check that the percentile values are identical
    histogram1x = HdrHistogram(LOWEST, IMPORTED_MAX_LATENCY, SIGNIFICANT)
    assert(histogram1x.add_bucket_counts(latency_data))
    for perc in [0, 10, 20, 30, 40, 50, 60, 75, 90, 99, 99.9, 99.99, 99.999]:
        assert(histogram.get_value_at_percentile(perc) == histogram1x.get_value_at_percentile(perc))

def test_reset():
    histogram = HdrHistogram(LOWEST, IMPORTED_MAX_LATENCY, SIGNIFICANT)
    latency_data = IMPORTED_LATENCY_DATA0
    assert(histogram.add_bucket_counts(latency_data))
    histogram.reset()
    assert(histogram.get_total_count() == 0)
    assert(histogram.get_value_at_percentile(99.99) == 0)

def test_invalid_significant_figures():
    try:
        HdrHistogram(LOWEST, HIGHEST, -1)
        assert(False)
    except ValueError:
        pass
    try:
        HdrHistogram(LOWEST, HIGHEST, 6)
        assert(False)
    except ValueError:
        pass

def test_out_of_range_values():
    histogram = HdrHistogram(1, 1000, 4)
    assert(histogram.record_value(32767))
    assert(histogram.record_value(32768) is False)
