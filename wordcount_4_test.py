from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from avg_price_job import AvgPrice


class AvgPriceSysTest(unittest.TestCase):
    SAMPLE_DATA = [("Mark Rothko", 1000), ("Mark Rothko", 500), ("ABC", 100)]

    @staticmethod
    def create_data(p):
        return p | beam.Create(AvgPriceSysTest.SAMPLE_DATA)

    def test_pcol_fun(self):
        expected = [("Mark Rothko", 750.0), ("ABC", 100.0)]
        with TestPipeline() as p:
            input = AvgPriceSysTest.create_data(p)
            result = AvgPrice.computeAvgPerKey(input)
            assert_that(result, equal_to(expected))


