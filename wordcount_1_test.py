from __future__ import absolute_import

import unittest

from avg_price import AvgPrice


class AvgPriceUnitTest(unittest.TestCase):
    def test_avg_basic(self):
        data = [("foo", (20, 2)), ("bar", (20, 1))]
        expected = [("foo", 10.0), ("bar", 20.0)]
        result = [AvgPrice.avg(x) for x in data]
        self.assertEqual(result, expected)

    def test_avg_single(self):
        data = [("foo", (20, 2))]
        expected = [("foo", 10.0)]
        result = [AvgPrice.avg(x) for x in data]
        self.assertEqual(result, expected)
