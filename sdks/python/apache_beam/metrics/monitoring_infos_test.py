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

import unittest

from apache_beam.coders import coders
from apache_beam.coders import coder_impl
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.cells import _TDIGEST_AVAILABLE
from apache_beam.metrics.cells import CounterCell
from apache_beam.metrics.cells import DistributionCell
from apache_beam.metrics.cells import GaugeCell
from apache_beam.metrics.cells import HistogramCell
from apache_beam.metrics.cells import HistogramData
from apache_beam.metrics.cells import StringSetCell
from apache_beam.utils.histogram import Histogram
from apache_beam.utils.histogram import LinearBucket

if _TDIGEST_AVAILABLE:
  from fastdigest import TDigest


class MonitoringInfosTest(unittest.TestCase):
  def test_parse_namespace_and_name_for_nonuser_metric(self):
    input = monitoring_infos.create_monitoring_info(
        "beam:dummy:metric", "typeurn", None)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "beam")
    self.assertEqual(name, "dummy:metric")

  def test_parse_namespace_and_name_for_user_counter_metric(self):
    urn = monitoring_infos.USER_COUNTER_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    labels[monitoring_infos.NAME_LABEL] = "countername"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "counternamespace")
    self.assertEqual(name, "countername")

  def test_parse_namespace_and_name_for_user_distribution_metric(self):
    urn = monitoring_infos.USER_DISTRIBUTION_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    labels[monitoring_infos.NAME_LABEL] = "countername"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "counternamespace")
    self.assertEqual(name, "countername")

  def test_parse_namespace_and_name_for_user_gauge_metric(self):
    urn = monitoring_infos.USER_GAUGE_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    labels[monitoring_infos.NAME_LABEL] = "countername"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "counternamespace")
    self.assertEqual(name, "countername")

  def test_parse_namespace_and_name_for_user_string_set_metric(self):
    urn = monitoring_infos.USER_STRING_SET_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "stringsetnamespace"
    labels[monitoring_infos.NAME_LABEL] = "stringsetname"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "stringsetnamespace")
    self.assertEqual(name, "stringsetname")

  def test_parse_namespace_and_name_for_user_histogram_metric(self):
    urn = monitoring_infos.USER_HISTOGRAM_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "histogramnamespace"
    labels[monitoring_infos.NAME_LABEL] = "histogramname"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(name, "histogramname")
    self.assertEqual(namespace, "histogramnamespace")

  def test_int64_user_gauge(self):
    metric = GaugeCell().get_cumulative()
    result = monitoring_infos.int64_user_gauge(
        'gaugenamespace', 'gaugename', metric)
    _, gauge_value = monitoring_infos.extract_gauge_value(result)
    self.assertEqual(0, gauge_value)

  def test_int64_user_counter(self):
    expected_labels = {}
    expected_labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    expected_labels[monitoring_infos.NAME_LABEL] = "countername"

    metric = CounterCell().get_cumulative()
    result = monitoring_infos.int64_user_counter(
        'counternamespace', 'countername', metric)
    counter_value = monitoring_infos.extract_counter_value(result)

    self.assertEqual(0, counter_value)
    self.assertEqual(result.labels, expected_labels)

  def test_int64_counter(self):
    expected_labels = {}
    expected_labels[monitoring_infos.PCOLLECTION_LABEL] = "collectionname"
    expected_labels[monitoring_infos.PTRANSFORM_LABEL] = "ptransformname"
    expected_labels[monitoring_infos.SERVICE_LABEL] = "BigQuery"

    labels = {
        monitoring_infos.SERVICE_LABEL: "BigQuery",
    }
    metric = CounterCell().get_cumulative()
    result = monitoring_infos.int64_counter(
        monitoring_infos.API_REQUEST_COUNT_URN,
        metric,
        ptransform="ptransformname",
        pcollection="collectionname",
        labels=labels)
    counter_value = monitoring_infos.extract_counter_value(result)

    self.assertEqual(0, counter_value)
    self.assertEqual(result.labels, expected_labels)

  def test_user_set_string(self):
    expected_labels = {}
    expected_labels[monitoring_infos.NAMESPACE_LABEL] = "stringsetnamespace"
    expected_labels[monitoring_infos.NAME_LABEL] = "stringsetname"

    metric = StringSetCell().get_cumulative()
    result = monitoring_infos.user_set_string(
        'stringsetnamespace', 'stringsetname', metric)
    string_set_value = monitoring_infos.extract_string_set_value(result)

    self.assertEqual(set(), string_set_value)
    self.assertEqual(result.labels, expected_labels)

  def test_user_histogram(self):
    datapoints = [5, 50, 90]
    expected_labels = {}
    expected_labels[monitoring_infos.NAMESPACE_LABEL] = "histogramnamespace"
    expected_labels[monitoring_infos.NAME_LABEL] = "histogramname"

    cell = HistogramCell(LinearBucket(0, 1, 100))
    for point in datapoints:
      cell.update(point)
    metric = cell.get_cumulative()
    result = monitoring_infos.user_histogram(
        'histogramnamespace', 'histogramname', metric)
    histogramvalue = monitoring_infos.extract_histogram_value(result)

    self.assertEqual(result.labels, expected_labels)
    exp_histogram = Histogram(LinearBucket(0, 1, 100))
    for point in datapoints:
      exp_histogram.record(point)
    self.assertEqual(HistogramData(exp_histogram), histogramvalue)


@unittest.skipUnless(_TDIGEST_AVAILABLE, 'fastdigest not installed')
class TDigestSerializationTest(unittest.TestCase):
  """Tests for TDigest serialization in monitoring_infos."""
  def test_encode_decode_distribution_with_tdigest(self):
    """Test encode/decode round-trip with tdigest."""
    # Create a tdigest with some values
    t = TDigest.from_values([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Encode
    payload = monitoring_infos._encode_distribution(
        coders.VarIntCoder(), 10, 55, 1, 10, t)

    # Decode
    result = monitoring_infos._decode_distribution(
        coders.VarIntCoder(), payload)
    count, sum_val, min_val, max_val, tdigest = result

    self.assertEqual(count, 10)
    self.assertEqual(sum_val, 55)
    self.assertEqual(min_val, 1)
    self.assertEqual(max_val, 10)
    self.assertIsNotNone(tdigest)
    self.assertAlmostEqual(tdigest.quantile(0.5), t.quantile(0.5), delta=0.01)

  def test_decode_distribution_without_tdigest_backwards_compat(self):
    """Test decoding old-format payload without tdigest bytes."""
    # Manually create old-format payload (no tdigest bytes)
    count_coder = coders.VarIntCoder().get_impl()
    value_coder = coders.VarIntCoder().get_impl()
    stream = coder_impl.create_OutputStream()
    count_coder.encode_to_stream(10, stream, True)
    value_coder.encode_to_stream(55, stream, True)
    value_coder.encode_to_stream(1, stream, True)
    value_coder.encode_to_stream(10, stream, True)
    old_payload = stream.get()

    # Decode should work and return None for tdigest
    result = monitoring_infos._decode_distribution(
        coders.VarIntCoder(), old_payload)

    self.assertEqual(result[0], 10)  # count
    self.assertEqual(result[1], 55)  # sum
    self.assertEqual(result[2], 1)  # min
    self.assertEqual(result[3], 10)  # max
    self.assertIsNone(result[4])  # tdigest should be None

  def test_int64_user_distribution_includes_tdigest(self):
    """Test that int64_user_distribution includes tdigest."""
    cell = DistributionCell()
    for i in range(1, 11):
      cell.update(i)

    data = cell.get_cumulative()
    mi = monitoring_infos.int64_user_distribution(
        'test_ns', 'test_name', data, ptransform='test_transform')

    # Extract and verify
    result = monitoring_infos.extract_metric_result_map_value(mi)

    self.assertIsNotNone(result)
    self.assertEqual(result.count, 10)
    self.assertIsNotNone(result.data.tdigest)
    self.assertAlmostEqual(result.p50, 5.5, delta=1)

  def test_distribution_payload_combiner_merges_tdigests(self):
    """Test that distribution_payload_combiner merges tdigests."""
    t1 = TDigest.from_values([1, 2, 3, 4, 5])
    t2 = TDigest.from_values([6, 7, 8, 9, 10])

    payload_a = monitoring_infos._encode_distribution(
        coders.VarIntCoder(), 5, 15, 1, 5, t1)
    payload_b = monitoring_infos._encode_distribution(
        coders.VarIntCoder(), 5, 40, 6, 10, t2)

    combined = monitoring_infos.distribution_payload_combiner(
        payload_a, payload_b)

    result = monitoring_infos._decode_distribution(
        coders.VarIntCoder(), combined)
    count, sum_val, min_val, max_val, tdigest = result

    self.assertEqual(count, 10)
    self.assertEqual(sum_val, 55)
    self.assertEqual(min_val, 1)
    self.assertEqual(max_val, 10)
    self.assertIsNotNone(tdigest)
    self.assertAlmostEqual(tdigest.quantile(0.5), 5.5, delta=1)


if __name__ == '__main__':
  unittest.main()
