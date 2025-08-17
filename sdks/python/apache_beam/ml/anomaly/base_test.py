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

import copy
import logging
import unittest

from parameterized import parameterized

from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.base import ThresholdFn
from apache_beam.ml.anomaly.specifiable import _KNOWN_SPECIFIABLE
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import specifiable


class TestAnomalyDetector(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  @parameterized.expand([(False, False), (True, False), (False, True),
                         (True, True)])
  def test_model_id_and_spec(self, on_demand_init, just_in_time_init):
    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class DummyThreshold(ThresholdFn):
      def __init__(self, my_threshold_arg=None):
        ...

      def is_stateful(self):
        return False

      def threshold(self):
        ...

      def apply(self, x):
        ...

    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class Dummy(AnomalyDetector):
      def __init__(self, my_arg=None, **kwargs):
        self._my_arg = my_arg
        super().__init__(**kwargs)

      def learn_one(self):
        ...

      def score_one(self):
        ...

      def __eq__(self, value) -> bool:
        return isinstance(value, Dummy) and \
          self._my_arg == value._my_arg

    a = Dummy(
        my_arg="abc",
        target="ABC",
        threshold_criterion=(t1 := DummyThreshold(2)))

    # The class attributes can only be accessed when
    # (1) on_demand_init == False, just_in_time_init == False
    #     In this case, the true __init__ is called immediately during object
    #     initialization
    # (2) just_in_time_init == True
    #     In this case, regardless what on_demand_init is, the true __init__
    #     is called when we first access any class attribute (delay init).
    if just_in_time_init or not on_demand_init:
      self.assertEqual(a._model_id, "Dummy")
      self.assertEqual(a._target, "ABC")
      self.assertEqual(a._my_arg, "abc")

    assert isinstance(a, Specifiable)
    self.assertEqual(
        a.init_kwargs, {
            "my_arg": "abc",
            "target": "ABC",
            "threshold_criterion": t1,
        })

    b = Dummy(
        my_arg="efg",
        model_id="my_dummy",
        target="EFG",
        threshold_criterion=(t2 := DummyThreshold(3)))

    # See the comment above for more details.
    if just_in_time_init or not on_demand_init:
      self.assertEqual(b._model_id, "my_dummy")
      self.assertEqual(b._target, "EFG")
      self.assertEqual(b._my_arg, "efg")

    assert isinstance(b, Specifiable)
    self.assertEqual(
        b.init_kwargs,
        {
            "model_id": "my_dummy",
            "my_arg": "efg",
            "target": "EFG",
            "threshold_criterion": t2,
        })

    spec = b.to_spec()
    expected_spec = Spec(
        type="Dummy",
        config={
            "my_arg": "efg",
            "model_id": "my_dummy",
            "target": "EFG",
            "threshold_criterion": Spec(
                type="DummyThreshold", config={"my_threshold_arg": 3}),
        })
    self.assertEqual(spec, expected_spec)

    b_dup = Specifiable.from_spec(spec)
    # We need to manually call the original __init__ function in one scenario.
    # See the comment above for more details.
    if on_demand_init and not just_in_time_init:
      b.run_original_init()

    self.assertEqual(b, b_dup)


class TestEnsembleAnomalyDetector(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  @parameterized.expand([(False, False), (True, False), (False, True),
                         (True, True)])
  def test_model_id_and_spec(self, on_demand_init, just_in_time_init):
    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class DummyAggregation(AggregationFn):
      def apply(self, x):
        ...

    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class DummyEnsemble(EnsembleAnomalyDetector):
      def __init__(self, my_ensemble_arg=None, **kwargs):
        super().__init__(**kwargs)
        self._my_ensemble_arg = my_ensemble_arg

      def learn_one(self):
        ...

      def score_one(self):
        ...

      def __eq__(self, value) -> bool:
        return isinstance(value, DummyEnsemble) and \
          self._my_ensemble_arg == value._my_ensemble_arg

    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class DummyWeakLearner(AnomalyDetector):
      def __init__(self, my_arg=None, **kwargs):
        super().__init__(**kwargs)
        self._my_arg = my_arg

      def learn_one(self):
        ...

      def score_one(self):
        ...

      def __eq__(self, value) -> bool:
        return isinstance(value, DummyWeakLearner) \
          and self._my_arg == value._my_arg

    # See the comment in TestAnomalyDetector for more details.
    if just_in_time_init or not on_demand_init:
      a = DummyEnsemble()
      self.assertEqual(a._model_id, "DummyEnsemble")

      b = DummyEnsemble(model_id="my_dummy_ensemble")
      self.assertEqual(b._model_id, "my_dummy_ensemble")

      c = EnsembleAnomalyDetector()
      self.assertEqual(c._model_id, "custom")

      d = EnsembleAnomalyDetector(model_id="my_dummy_ensemble_2")
      self.assertEqual(d._model_id, "my_dummy_ensemble_2")

    d1 = DummyWeakLearner(my_arg=1)
    d2 = DummyWeakLearner(my_arg=2)
    ensemble = DummyEnsemble(
        my_ensemble_arg=123,
        learners=[d1, d2],
        aggregation_strategy=DummyAggregation())

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

    ensemble_dup = Specifiable.from_spec(spec)

    # See the comment in TestAnomalyDetector for more details.
    if on_demand_init and not just_in_time_init:
      ensemble.run_original_init()

    self.assertEqual(ensemble, ensemble_dup)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
