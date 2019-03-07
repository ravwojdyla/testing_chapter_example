from __future__ import absolute_import

import logging
import re
import tempfile
import unittest

from avg_price_job import AvgPrice
from apache_beam.testing.util import open_shards


class AvgPriceIT(unittest.TestCase):
    SAMPLE_DATA = "Mark Rothko\t1000\nMark Rothko\t500\nABC\t100"

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents.encode("utf-8"))
            return f.name

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_DATA)
        expected_words = [("abc", 100), ("rothko", 750)]
        AvgPrice.run(["--input=%s*" % temp_path, "--output=%s.result" % temp_path])
        results = []
        with open_shards(temp_path + ".result-*-of-*") as result_file:
            for line in result_file:
                match = re.search(r"([a-zA-Z]+): ([0-9]+)", line)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
        self.assertEqual(sorted(results), sorted(expected_words))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
