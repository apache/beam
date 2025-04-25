#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Used for internal testing. No backwards compatibility.
"""

import argparse
import logging
import time
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference import base
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.userstate import CombiningValueStateSpec


# create some fake models which returns different inference results.
class FakeModelDefault:
  def predict(self, example: int) -> int:
    return example


class FakeModelAdd(FakeModelDefault):
  def predict(self, example: int) -> int:
    return example + 1


class FakeModelSub(FakeModelDefault):
  def predict(self, example: int) -> int:
    return example - 1


class FakeModelHandlerReturnsPredictionResult(
    base.ModelHandler[int, base.PredictionResult, FakeModelDefault]):
  def __init__(self, clock=None, model_id='model_default'):
    self.model_id = model_id
    self._fake_clock = clock

  def load_model(self):
    if self._fake_clock:
      self._fake_clock.current_time_ns += 500_000_000  # 500ms
    if self.model_id == 'model_add.pkl':
      return FakeModelAdd()
    elif self.model_id == 'model_sub.pkl':
      return FakeModelSub()
    return FakeModelDefault()

  def run_inference(
      self,
      batch: Sequence[int],
      model: FakeModelDefault,
      inference_args=None) -> Iterable[base.PredictionResult]:
    for example in batch:
      yield base.PredictionResult(
          model_id=self.model_id,
          example=example,
          inference=model.predict(example))

  def update_model_path(self, model_path: Optional[str] = None):
    self.model_id = model_path if model_path else self.model_id


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  first_ts = time.time()
  side_input_interval = 60
  main_input_interval = 20
  # give some time for dataflow to start.
  last_ts = first_ts + 1200
  mid_ts = (first_ts + last_ts) / 2

  _, pipeline_args = parser.parse_known_args(argv)
  options = PipelineOptions(pipeline_args)
  options.view_as(SetupOptions).save_main_session = save_main_session

  class GetModel(beam.DoFn):
    def process(self, element) -> Iterable[base.ModelMetadata]:
      if time.time() > mid_ts:
        yield base.ModelMetadata(
            model_id='model_add.pkl', model_name='model_add')
      else:
        yield base.ModelMetadata(
            model_id='model_sub.pkl', model_name='model_sub')

  class _EmitSingletonSideInput(beam.DoFn):
    COUNT_STATE = CombiningValueStateSpec('count', combine_fn=sum)

    def process(self, element, count_state=beam.DoFn.StateParam(COUNT_STATE)):
      _, path = element
      counter = count_state.read()
      if counter == 0:
        count_state.add(1)
        yield path

  def validate_prediction_result(x: base.PredictionResult):
    model_id = x.model_id
    if model_id == 'model_sub.pkl':
      assert (x.example == 1 and x.inference == 0)

    if model_id == 'model_add.pkl':
      assert (x.example == 1 and x.inference == 2)

    if model_id == 'model_default':
      assert (x.example == 1 and x.inference == 1)

  with beam.Pipeline(options=options) as pipeline:
    side_input = (
        pipeline
        | "SideInputPColl" >> PeriodicImpulse(
            first_ts, last_ts, fire_interval=side_input_interval)
        | "GetModelId" >> beam.ParDo(GetModel())
        | "AttachKey" >> beam.Map(lambda x: (x, x))
        # due to periodic impulse, which has a start timestamp before
        # Dataflow pipeline process data, it can trigger in multiple
        # firings, causing an Iterable instead of singleton. So, using
        # the _EmitSingletonSideInput DoFn will ensure unique path will be
        # fired only once.
        | "GetSingleton" >> beam.ParDo(_EmitSingletonSideInput())
        | "ApplySideInputWindow" >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING))

    model_handler = FakeModelHandlerReturnsPredictionResult()
    inference_pcoll = (
        pipeline
        | "MainInputPColl" >> PeriodicImpulse(
            first_ts,
            last_ts,
            fire_interval=main_input_interval,
            apply_windowing=True)
        | beam.Map(lambda x: 1)
        | base.RunInference(
            model_handler=model_handler, model_metadata_pcoll=side_input))

    _ = inference_pcoll | "AssertPredictionResult" >> beam.Map(
        validate_prediction_result)

    _ = inference_pcoll | "Logging" >> beam.Map(logging.info)


if __name__ == '__main__':
  run()
