import argparse
import logging
import os
import tempfile
import time

import apache_beam as beam

from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.ml.transforms.tft import TFIDF

# Names of temp files
SHUFFLED_TRAIN_DATA_FILEBASE = 'train_shuffled'
SHUFFLED_TEST_DATA_FILEBASE = 'test_shuffled'
REVIEW_COLUMN = 'review'
LABEL_COLUMN = 'label'
DELIMITERS = '.,!?() '
VOCAB_SIZE = 20000


# pylint: disable=invalid-name
@beam.ptransform_fn
def Shuffle(pcoll):
  """Shuffles a PCollection.  Collection should not contain duplicates."""
  return (
      pcoll
      | 'PairWithHash' >> beam.Map(lambda x: (hash(x), x))
      | 'GroupByHash' >> beam.GroupByKey()
      | 'DropHash' >> beam.FlatMap(lambda hash_and_values: hash_and_values[1]))


class ReadAndShuffleData(beam.PTransform):
  def __init__(self, pos_file_pattern, neg_file_pattern):
    self.pos_file_pattern = pos_file_pattern
    self.neg_file_pattern = neg_file_pattern

  def expand(self, pcoll):

    negative_examples = (
        pcoll
        | "ReadNegativeExample" >> beam.io.ReadFromText(self.neg_file_pattern)
        | 'PairWithZero' >> beam.Map(lambda review: (review, 0)))

    positive_examples = (
        pcoll
        | "ReadPositiveExample" >> beam.io.ReadFromText(self.pos_file_pattern)
        | 'PairWithOne' >> beam.Map(lambda review: (review, 1)))

    all_examples = ((negative_examples, positive_examples)
                    | 'FlattenPColls' >> beam.Flatten())

    shuffled_examples = (
        all_examples
        | 'Distinct' >> beam.Distinct()
        | 'Shuffle' >> Shuffle())

    # tag with column names for MLTransform
    return (
        shuffled_examples
        | beam.Map(
            lambda label_review: {
                REVIEW_COLUMN: label_review[0], LABEL_COLUMN: label_review[1]
            }))


def preprocess_data(file_patterns, pipeline_args, working_dir):
  positive_pattern, negative_pattern = file_patterns
  artifact_location = working_dir
  options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
  with beam.Pipeline(options=options) as pipeline:
    data_pcoll = (
        pipeline
        | 'ReadTrainData' >> ReadAndShuffleData(
            positive_pattern, negative_pattern))
    ml_transform = MLTransform(
        artifact_location=artifact_location,
    ).with_transform(
        ComputeAndApplyVocabulary(
            columns=[REVIEW_COLUMN],
            split_string_by_delimiter=DELIMITERS,
            top_k=VOCAB_SIZE)).with_transform(
                TFIDF(columns=[REVIEW_COLUMN], vocab_size=VOCAB_SIZE + 1))
    data_pcoll = data_pcoll | 'MLTransform' >> ml_transform
    return data_pcoll


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_data_dir', help='path to directory containing input data')
  parser.add_argument(
      '--working_dir',
      help='path to directory to hold transformed data',
      default=None)
  args, pipeline_args = parser.parse_known_args()

  neg_filepatterm = os.path.join(args.input_data_dir, 'train/neg/*')
  pos_filepattern = os.path.join(args.input_data_dir, 'train/pos/*')

  working_dir = args.working_dir
  if not working_dir:
    working_dir = tempfile.mkdtemp(dir=args.input_data_dir)

  preprocess_data((pos_filepattern, neg_filepatterm),
                  pipeline_args,
                  working_dir)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  start_time = time.time()
  main()
