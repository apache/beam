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

"""This module contains Splittable DoFn logic that's common to all runners."""

from __future__ import absolute_import

import uuid
from builtins import object

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import typecoders
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.common import DoFnInvoker
from apache_beam.runners.common import DoFnSignature
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.ptransform import PTransform


class SplittableParDoOverride(PTransformOverride):
  """A transform override for ParDo transformss of SplittableDoFns.

  Replaces the ParDo transform with a SplittableParDo transform that performs
  SDF specific logic.
  """

  def matches(self, applied_ptransform):
    assert isinstance(applied_ptransform, AppliedPTransform)
    transform = applied_ptransform.transform
    if isinstance(transform, ParDo):
      signature = DoFnSignature(transform.fn)
      return signature.is_splittable_dofn()

  def get_replacement_transform(self, ptransform):
    assert isinstance(ptransform, ParDo)
    do_fn = ptransform.fn
    signature = DoFnSignature(do_fn)
    if signature.is_splittable_dofn():
      return SplittableParDo(ptransform)
    else:
      return ptransform


class SplittableParDo(PTransform):
  """A transform that processes a PCollection using a Splittable DoFn."""

  def __init__(self, ptransform):
    assert isinstance(ptransform, ParDo)
    self._ptransform = ptransform

  def expand(self, pcoll):
    sdf = self._ptransform.fn
    signature = DoFnSignature(sdf)
    invoker = DoFnInvoker.create_invoker(signature, process_invocation=False)

    element_coder = typecoders.registry.get_coder(pcoll.element_type)
    restriction_coder = invoker.invoke_restriction_coder()

    keyed_elements = (pcoll
                      | 'pair' >> ParDo(PairWithRestrictionFn(sdf))
                      | 'split' >> ParDo(SplitRestrictionFn(sdf))
                      | 'explode' >> ParDo(ExplodeWindowsFn())
                      | 'random' >> ParDo(RandomUniqueKeyFn()))

    return keyed_elements | ProcessKeyedElements(
        sdf, element_coder, restriction_coder,
        pcoll.windowing, self._ptransform.args, self._ptransform.kwargs,
        self._ptransform.side_inputs)


class ElementAndRestriction(object):
  """A holder for an element and a restriction."""

  def __init__(self, element, restriction):
    self.element = element
    self.restriction = restriction


class PairWithRestrictionFn(beam.DoFn):
  """A transform that pairs each element with a restriction."""

  def __init__(self, do_fn):
    self._do_fn = do_fn

  def start_bundle(self):
    signature = DoFnSignature(self._do_fn)
    self._invoker = DoFnInvoker.create_invoker(
        signature, process_invocation=False)

  def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
    initial_restriction = self._invoker.invoke_initial_restriction(element)
    yield ElementAndRestriction(element, initial_restriction)


class SplitRestrictionFn(beam.DoFn):
  """A transform that perform initial splitting of Splittable DoFn inputs."""

  def __init__(self, do_fn):
    self._do_fn = do_fn

  def start_bundle(self):
    signature = DoFnSignature(self._do_fn)
    self._invoker = DoFnInvoker.create_invoker(
        signature, process_invocation=False)

  def process(self, element_and_restriction, *args, **kwargs):
    element = element_and_restriction.element
    restriction = element_and_restriction.restriction
    restriction_parts = self._invoker.invoke_split(
        element,
        restriction)
    for part in restriction_parts:
      yield ElementAndRestriction(element, part)


class ExplodeWindowsFn(beam.DoFn):
  """A transform that forces the runner to explode windows.

  This is done to make sure that Splittable DoFn proceses an element for each of
  the windows that element belongs to.
  """

  def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
    yield element


class RandomUniqueKeyFn(beam.DoFn):
  """A transform that assigns a unique key to each element."""

  def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
    # We ignore UUID collisions here since they are extremely rare.
    yield (uuid.uuid4().bytes, element)


class ProcessKeyedElements(PTransform):
  """A primitive transform that performs SplittableDoFn magic.

  Input to this transform should be a PCollection of keyed ElementAndRestriction
  objects.
  """

  def __init__(
      self, sdf, element_coder, restriction_coder, windowing_strategy,
      ptransform_args, ptransform_kwargs, ptransform_side_inputs):
    self.sdf = sdf
    self.element_coder = element_coder
    self.restriction_coder = restriction_coder
    self.windowing_strategy = windowing_strategy
    self.ptransform_args = ptransform_args
    self.ptransform_kwargs = ptransform_kwargs
    self.ptransform_side_inputs = ptransform_side_inputs

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline)
