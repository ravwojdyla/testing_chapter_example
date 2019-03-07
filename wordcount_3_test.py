from __future__ import absolute_import

import unittest

from hypothesis import given, reproduce_failure
from hypothesis.strategies import text, lists, tuples, integers

from avg_price import AvgPrice


class AvgPriceUnitTest(unittest.TestCase):

    @given(lists(tuples(text(), tuples(integers(min_value=0), integers(min_value=0)))))
    def test_avg(self, s):
        result = [AvgPrice.avg(x) for x in s]
