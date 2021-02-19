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

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.dataframe.io import read_csv
import pandas as pd

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie

from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection

from typing import NamedTuple
import random

class DataFrameWrapper:
  def __init__(self, method, p):
    self.method = method
    self.p = p

  def __call__(self, *args, **kwargs):
    cleaned_args = [a.data if isinstance(a, DataFrame) else a for a in args]
    return DataFrame(self.method(*cleaned_args, **kwargs), p=self.p)


class DataFrame:
  def __init__(self, data, runner=InteractiveRunner, p=None):
    self.p = p
    if isinstance(data, beam.PTransform):
      self.data = DataFrame(self.p | data).data
    elif isinstance(data, beam.PCollection):
      self.data = to_dataframe(data)
    elif isinstance(data, (dict, tuple, list)):
      self.data = to_dataframe(self.p | beam.Create(data))
    else:
      self.data = data

    # Prototype only code, change this.
    def create_operator(op):
      return lambda a, b: DataFrame(getattr(a.data, op)(b.data), p=a.p)

    for op in ('__add__', '__sub__', '__mul__', '__pow__',
               '__truediv__', '__floordiv__', '__mod__',
               '__and__', '__or__', '__xor__', '__invert__',
               '__lt__', '__le__', '__eq__', '__ne__', '__gt__',
               '__ge__'):
      setattr(DataFrame, op, create_operator(op))

  def __getitem__(self, k):
    return DataFrame(self.data[k], p=self.p)

  def __getattr__(self, k):
    return DataFrameWrapper(getattr(self.data, k), p=self.p)

  def head(self, n=10):
    return ib.collect(to_pcollection(self.data, label='head'), n=n)

  def collect(self, n):
    return ib.collect(to_pcollection(self.data), n=n)

  def __repr__(self):
    return str(self.head())
