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
# pytype: skip-file

"""
Util/helper functions used in apache_beam.ml.inference.
"""
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Union

from apache_beam.ml.inference.base import PredictionResult


def _convert_to_result(
    batch: Iterable,
    predictions: Union[Iterable, Dict[Any, Iterable]],
    model_id: Optional[str] = None,
) -> Iterable[PredictionResult]:
  if isinstance(predictions, dict):
    # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
    # key_type2: Iterable<val_type2>, ...} where each Iterable is of
    # length batch_size, to a list of dictionaries:
    # [{key_type1: value_type1, key_type2: value_type2}]
    predictions_per_tensor = [
        dict(zip(predictions.keys(), v)) for v in zip(*predictions.values())
    ]
    return [
        PredictionResult(x, y, model_id) for x,
        y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y, model_id) for x, y in zip(batch, predictions)]
