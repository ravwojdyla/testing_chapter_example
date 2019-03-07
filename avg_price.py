from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AvgPrice:
    @staticmethod
    def add(iterable):
        it = iter(iterable)
        result, rest = (next(it), it)
        for x, y in rest:
            result = (result[0] + x, result[1] + y)
        return result

    @staticmethod
    def avg(name_to_sum_and_count):
        (name, (sum, count)) = name_to_sum_and_count
        return name, sum / count

    @staticmethod
    def run(argv=None):
        parser = argparse.ArgumentParser()
        parser.add_argument("--input", dest="input", default="/tmp/auctions.txt")
        parser.add_argument("--output", dest="output", required=True)
        known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_args.extend(
            [
                "--runner=DirectRunner",
                "--temp_location=/tmp/beam_tmp",
                "--job_name=test-job",
            ]
        )

        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options.view_as(SetupOptions).save_main_session = True
        with beam.Pipeline(options=pipeline_options) as p:

            lines = p | ReadFromText(known_args.input)

            counts = (
                lines
                | "Split"
                >> (
                    beam.Map(lambda x: tuple(x.split("\t"))).with_output_types(
                        beam.typehints.Tuple[str, str]
                    )
                )
                | "Clean" >> beam.Map(lambda x: (x[0].strip(), int(x[1].strip())))
                | "PairWithOne" >> beam.Map(lambda x: (x[0], (x[1], 1)))
                | "GroupAndSum" >> beam.CombinePerKey(AvgPrice.add)
                | "Avg" >> beam.Map(AvgPrice.avg)
            )

            def format_result(word_count):
                (word, count) = word_count
                return "%s: %s" % (word, count)

            output = counts | "Format" >> beam.Map(format_result)
            output | WriteToText(known_args.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    AvgPrice.run()
