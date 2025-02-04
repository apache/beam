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

from __future__ import annotations

import logging
import unittest

from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.base import ThresholdFn
from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import specifiable


class TestAnomalyDetector(unittest.TestCase):
  @specifiable(on_demand_init=False)
  class DummyThreshold(ThresholdFn):
    def __init__(self, my_threshold_arg=None):
      ...

    def is_stateful(self):
      return False

    def threshold(self):
      ...

    def apply(self, x):
      ...

  @specifiable(on_demand_init=False)
  class Dummy(AnomalyDetector):
    def __init__(self, my_arg=None, **kwargs):
      self._my_arg = my_arg
      super().__init__(**kwargs)

    def learn_one(self):
      ...

    def score_one(self):
      ...

    def __eq__(self, value: "TestAnomalyDetector.Dummy") -> bool:
      return self._my_arg == value._my_arg

  def test_unknown_detector(self):
    self.assertRaises(ValueError, Specifiable.from_spec, Spec(type="unknown"))

  def test_model_id_on_known_detector(self):
    a = self.Dummy(
        my_arg="abc",
        target="ABC",
        threshold_criterion=(t1 := self.DummyThreshold(2)))

    self.assertEqual(a._model_id, "Dummy")
    self.assertEqual(a._target, "ABC")
    self.assertEqual(a._my_arg, "abc")

    assert isinstance(a, Specifiable)
    self.assertEqual(
        a._init_params, {
            "my_arg": "abc",
            "target": "ABC",
            "threshold_criterion": t1,
        })

    b = self.Dummy(
        my_arg="efg",
        model_id="my_dummy",
        target="EFG",
        threshold_criterion=(t2 := self.DummyThreshold(2)))
    self.assertEqual(b._model_id, "my_dummy")
    self.assertEqual(b._target, "EFG")
    self.assertEqual(b._my_arg, "efg")

    assert isinstance(b, Specifiable)
    self.assertEqual(
        b._init_params,
        {
            "model_id": "my_dummy",
            "my_arg": "efg",
            "target": "EFG",
            "threshold_criterion": t2,
        })

  def test_from_and_to_specifiable(self):
    obj = self.Dummy(
        my_arg="hij",
        model_id="my_dummy",
        target="HIJ",
        threshold_criterion=self.DummyThreshold(4))

    assert isinstance(obj, Specifiable)
    spec = obj.to_spec()
    expected_spec = Spec(
        type="Dummy",
        config={
            "my_arg": "hij",
            "model_id": "my_dummy",
            "target": "HIJ",
            "threshold_criterion": Spec(
                type="DummyThreshold", config={"my_threshold_arg": 4}),
        })
    self.assertEqual(spec, expected_spec)

    new_obj = Specifiable.from_spec(spec)
    self.assertEqual(obj, new_obj)


class TestEnsembleAnomalyDetector(unittest.TestCase):
  @specifiable(on_demand_init=False)
  class DummyAggregation(AggregationFn):
    def apply(self, x):
      ...

  @specifiable(on_demand_init=False)
  class DummyEnsemble(EnsembleAnomalyDetector):
    def __init__(self, my_ensemble_arg=None, **kwargs):
      super().__init__(**kwargs)
      self._my_ensemble_arg = my_ensemble_arg

    def learn_one(self):
      ...

    def score_one(self):
      ...

    def __eq__(
        self, value: 'TestEnsembleAnomalyDetector.DummyEnsemble') -> bool:
      return self._my_ensemble_arg == value._my_ensemble_arg

  @specifiable(on_demand_init=False)
  class DummyWeakLearner(AnomalyDetector):
    def __init__(self, my_arg=None, **kwargs):
      super().__init__(**kwargs)
      self._my_arg = my_arg

    def learn_one(self):
      ...

    def score_one(self):
      ...

    def __eq__(
        self, value: 'TestEnsembleAnomalyDetector.DummyWeakLearner') -> bool:
      return self._my_arg == value._my_arg

  def test_model_id_on_known_detector(self):
    a = self.DummyEnsemble()
    self.assertEqual(a._model_id, "DummyEnsemble")

    b = self.DummyEnsemble(model_id="my_dummy_ensemble")
    self.assertEqual(b._model_id, "my_dummy_ensemble")

    c = EnsembleAnomalyDetector()
    self.assertEqual(c._model_id, "custom")

    d = EnsembleAnomalyDetector(model_id="my_dummy_ensemble_2")
    self.assertEqual(d._model_id, "my_dummy_ensemble_2")

  def test_from_and_to_specifiable(self):
    d1 = self.DummyWeakLearner(my_arg=1)
    d2 = self.DummyWeakLearner(my_arg=2)
    ensemble = self.DummyEnsemble(
        my_ensemble_arg=123,
        learners=[d1, d2],
        aggregation_strategy=self.DummyAggregation())

    expected_spec = Spec(
        type="DummyEnsemble",
        config={
            "my_ensemble_arg": 123,
            "learners": [
                Spec(type="DummyWeakLearner", config={"my_arg": 1}),
                Spec(type="DummyWeakLearner", config={"my_arg": 2})
            ],
            "aggregation_strategy": Spec(
                type="DummyAggregation",
                config={},
            ),
        })

    assert isinstance(ensemble, Specifiable)
    spec = ensemble.to_spec()
    self.assertEqual(spec, expected_spec)

    new_ensemble = Specifiable.from_spec(spec)
    self.assertEqual(ensemble, new_ensemble)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
