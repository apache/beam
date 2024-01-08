# # # coding=utf-8
# # #
# # # Licensed to the Apache Software Foundation (ASF) under one or more
# # # contributor license agreements.  See the NOTICE file distributed with
# # # this work for additional information regarding copyright ownership.
# # # The ASF licenses this file to You under the Apache License, Version 2.0
# # # (the "License"); you may not use this file except in compliance with
# # # the License.  You may obtain a copy of the License at
# # #
# # #    http://www.apache.org/licenses/LICENSE-2.0
# # #
# # # Unless required by applicable law or agreed to in writing, software
# # # distributed under the License is distributed on an "AS IS" BASIS,
# # # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # # See the License for the specific language governing permissions and
# # # limitations under the License.
# # #

# # # pytype: skip-file
# # # pylint:disable=line-too-long

# # from apache_beam.transforms import combiners
# # from apache_beam.runners.portability.fn_api_runner import FnApiRunner
# # from apache_beam.transforms.combinefn_lifecycle_pipeline import CallSequenceEnforcingCombineFn

# # def combineglobally_combinefn(test=None):
# #   import apache_beam as beam

# #   # class PercentagesFn(beam.CombineFn):
# #     # def create_accumulator(self):
# #     #   return {}

# #     # def add_input(self, accumulator, input):
# #     #   # accumulator == {}
# #     #   # input == '🥕'
# #     #   if input not in accumulator:
# #     #     accumulator[input] = 0  # {'🥕': 0}
# #     #   accumulator[input] += 1  # {'🥕': 1}
# #     #   return accumulator

# #     # def merge_accumulators(self, accumulators):
# #     #   # accumulators == [
# #     #   #     {'🥕': 1, '🍅': 2},
# #     #   #     {'🥕': 1, '🍅': 1, '🍆': 1},
# #     #   #     {'🥕': 1, '🍅': 3},
# #     #   # ]
# #     #   merged = {}
# #     #   for accum in accumulators:
# #     #     for item, count in accum.items():
# #     #       if item not in merged:
# #     #         merged[item] = 0
# #     #       merged[item] += count
# #     #   # merged == {'🥕': 3, '🍅': 6, '🍆': 1}
# #     #   return merged

# #     # def extract_output(self, accumulator):
# #     #   # accumulator == {'🥕': 3, '🍅': 6, '🍆': 1}
# #     #   total = sum(accumulator.values())  # 10
# #     #   percentages = {item: count / total for item, count in accumulator.items()}
# #     #   # percentages == {'🥕': 0.3, '🍅': 0.6, '🍆': 0.1}
# #     #   return percentages


# #   with beam.Pipeline(runner=FnApiRunner()) as p:
# #     pcoll = p | 'Start' >> beam.Create(range(5))
# #     pcoll |= 'Do' >> beam.CombineGlobally(
# #         combiners.SingleInputTupleCombineFn(
# #             CallSequenceEnforcingCombineFn(), CallSequenceEnforcingCombineFn()),
# #         None).with_fanout(fanout=1)

# #     pcoll | beam.Map(print)

# # if __name__ == '__main__':
# #   import logging
# #   logging.getLogger().setLevel(logging.INFO)
# #   combineglobally_combinefn()


# # """
# # Next steps:
# # 1. Debug the pipeline dag.
# # 2. Check stages. Why does the CombineFn setup() is called twice?
# # """


# def _map_errors_to_standard_format():
# # TODO(https://github.com/apache/beam/issues/24755): Switch to MapTuple.
#   return beam.Map(
#       lambda x: beam.Row(element=x[0], msg=str(x[1][1]), stack=str(x[1][2])))


# import apache_beam as beam
# from apache_beam.yaml.yaml_mapping import maybe_with_exception_handling

# def assert_error(ele):
#   if not isinstance(ele, int):
#     raise ValueError("Expected int type. Received str type")
#   return ele

# class MyTransform(beam.PTransform):
#   def __init__(self):
#     self._exception_handling_args = {'main_tag': 'good', 'dead_letter_tag': 'bad'}
#   @maybe_with_exception_handling
#   def expand(self, pcoll):
#     # wrapper_pcoll = beam.core._MaybePValueWithErrors(pcoll, self._exception_handling_args)
#     # pcoll = wrapper_pcoll | beam.Map(assert_error)
#     # return pcoll.as_result(_map_errors_to_standard_format())
#     return pcoll | beam.Map(assert_error)

#   def with_exception_handling(self, **kwargs):
#     # self._exception_handling_args = kwargs
#     return self




# with beam.Pipeline() as p:

#   error_pcoll = (p | beam.Create([
#     1, 2, 3, 'hello', 4, 'world'
#   ])
#   | MyTransform()
#   )

#   error_pcoll['bad'] | beam.Map(print)
#   # bad | beam.Map(print)

# # def print_decorator(func):

# #   def wrapper(a, b):
# #     print(func(a, b))
# #     print('I am the wrapper')

# #   return wrapper

# # @print_decorator
# # def add(a, b):
# #   return a + b

# # add(1, 2)




import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings


model_name = 'sentence-transformers/paraphrase-MiniLM-L6-v2'

data = [
  {
    'text': 'I like to eat tomatoes',
    'label': 'tomato'
  },
  {
    'text': 'I like to eat eggplants',
    'label': 'eggplant'
  },
  {
    'text' : 2,
    'label': 'carrot'
  }
]


import tempfile
artifact_location = tempfile.mkdtemp(prefix='artifact_location')
with beam.Pipeline() as p:
  pcoll = p | beam.Create(data)

  good, bad = (
    pcoll
    | MLTransform(write_artifact_location=artifact_location).with_transform(
      SentenceTransformerEmbeddings(model_name=model_name, columns=['text'])
    ).with_exception_handling()
    # | beam.Map(print)
  )

  # pcoll['bad'] | "Bad" >> beam.Map(print)
  good | beam.Map(lambda x: x['text']) | beam.Map(len) | beam.Map(print)
  bad | "bad" >> beam.Map(print)
