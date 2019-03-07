from __future__ import absolute_import

import unittest

from hypothesis import given, reproduce_failure, assume
import hypothesis.strategies as st

from avg_price_job import AvgPrice


class AvgPriceUnitTest(unittest.TestCase):
    @given(st.text())
    def test_format(self, s):
        assert AvgPrice.format_artis(s).islower()
