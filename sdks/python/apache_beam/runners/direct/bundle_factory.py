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

"""A factory that creates UncommittedBundles."""

from __future__ import absolute_import

from apache_beam import pvalue


class BundleFactory(object):
  """BundleFactory creates output bundles to be used by transform evaluators."""

  def create_bundle(self, output_pcollection):
    return Bundle(output_pcollection)

  def create_empty_committed_bundle(self, output_pcollection):
    bundle = self.create_bundle(output_pcollection)
    bundle.commit(None)
    return bundle


# a bundle represents a unit of work that will be processed by a transform.
class Bundle(object):
  """Part of a PCollection with output elements.

  Part of a PCollection. Elements are output to a bundle, which will cause them
  to be executed by PTransform that consume the PCollection this bundle is a
  part of at a later point. It starts as an uncommitted bundle and can have
  elements added to it. It needs to be committed to make it immutable before
  passing it to a downstream ptransform.
  """

  def __init__(self, pcollection):
    assert (isinstance(pcollection, pvalue.PCollection)
            or isinstance(pcollection, pvalue.PCollectionView))
    self._pcollection = pcollection
    self._elements = []
    self._committed = False
    self._tag = None  # optional tag information for this bundle

  @property
  def elements(self):
    """Returns iterable elements. If not committed will return a copy."""
    if self._committed:
      return self._elements
    else:
      return list(self._elements)

  @property
  def tag(self):
    return self._tag

  @tag.setter
  def tag(self, value):
    assert not self._tag
    self._tag = value

  @property
  def pcollection(self):
    """PCollection that the elements of this UncommittedBundle belong to."""
    return self._pcollection

  def add(self, element):
    """Outputs an element to this bundle.

    Args:
      element: WindowedValue
    """
    assert not self._committed
    self._elements.append(element)

  def output(self, element):
    self.add(element)

  def commit(self, synchronized_processing_time):
    """Commits this bundle.

    Uncommitted bundle will become committed (immutable) after this call.

    Args:
      synchronized_processing_time: the synchronized processing time at which
      this bundle was committed
    """
    assert not self._committed
    self._committed = True
    self._elements = tuple(self._elements)
    self._synchronized_processing_time = synchronized_processing_time
