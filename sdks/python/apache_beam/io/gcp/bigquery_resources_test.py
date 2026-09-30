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

"""Unit tests for apache_beam.io.gcp.bigquery_resources."""

# pytype: skip-file

import copy
import dataclasses
import os
import pickle
import subprocess
import sys
import textwrap
import unittest
import warnings
from unittest import mock

from parameterized import parameterized

from apache_beam.coders import coders
from apache_beam.internal import cloudpickle_pickler
from apache_beam.internal import pickler
from apache_beam.io.gcp import bigquery_resources
from apache_beam.io.gcp.bigquery_resources import DatasetRef
from apache_beam.io.gcp.bigquery_resources import JobRef
from apache_beam.io.gcp.bigquery_resources import TableRef
from apache_beam.io.gcp.bigquery_resources import is_deferred
from apache_beam.io.gcp.bigquery_resources import to_dataset_ref
from apache_beam.io.gcp.bigquery_resources import to_job_ref
from apache_beam.io.gcp.bigquery_resources import to_schema_dict
from apache_beam.io.gcp.bigquery_resources import to_table_ref
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.utils.annotations import BeamDeprecationWarning

try:
  from apache_beam.internal import dill_pickler
except ImportError:
  dill_pickler = None  # type: ignore[assignment]

try:
  from apitools.base.py import encoding as apitools_encoding

  from apache_beam.io.gcp import bigquery_tools
  from apache_beam.io.gcp.internal.clients import bigquery as apitools_bigquery
except ImportError:
  apitools_encoding = None
  bigquery_tools = None
  apitools_bigquery = None

try:
  from google.cloud import bigquery as gcp_bigquery
  from google.cloud.bigquery.dataset import DatasetListItem
  from google.cloud.bigquery.table import TableListItem
except ImportError:
  gcp_bigquery = None
  DatasetListItem = None
  TableListItem = None

# Master's parse_table_reference needs both apitools and `regex`.
HAS_MASTER_PARSER = (
    apitools_bigquery is not None and bigquery_resources.regex is not None)

_SDK_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(bigquery_resources.__file__)))))


class _Error(object):
  """Marks a matrix case that must raise ValueError."""


ERR = _Error()

# (name, spec, dataset, project, expected with `regex`, expected with the
# stdlib `re` fallback). Expected values are (project, dataset_id, table_id)
# tuples or ERR.
_SAME = object()
PARSE_CASES = [
    # Inputs of master's TestTableReferenceParser, verbatim.
    (
        'master_fq_colon',
        'project:dataset.test_table',
        None,
        None, ('project', 'dataset', 'test_table'),
        _SAME),
    (
        'master_fq_dash',
        'project:dataset.test-table',
        None,
        None, ('project', 'dataset', 'test-table'),
        _SAME),
    (
        'master_fq_dash_space',
        'project:dataset.test- table',
        None,
        None, ('project', 'dataset', 'test- table'),
        _SAME),
    (
        'master_fq_dot_leading_space',
        'project.dataset. test_table',
        None,
        None, ('project', 'dataset', ' test_table'),
        _SAME),
    (
        'master_fq_dollar',
        'project.dataset.test$table',
        None,
        None, ('project', 'dataset', 'test$table'),
        _SAME),
    (
        'master_partially_qualified',
        'test_dataset.test_table',
        None,
        None, (None, 'test_dataset', 'test_table'),
        _SAME),
    ('master_insufficient', 'test_table', None, None, ERR, _SAME),
    (
        'master_all_arguments',
        'test_table',
        'test_dataset',
        'test_project', ('test_project', 'test_dataset', 'test_table'),
        _SAME),
    # Additional cases.
    (
        'fq_dot',
        'my-project.my_dataset.my_table',
        None,
        None, ('my-project', 'my_dataset', 'my_table'),
        _SAME),
    (
        'domain_scoped_colon',
        'google.com:my-project:my_dataset.my_table',
        None,
        None, ('google.com:my-project', 'my_dataset', 'my_table'),
        _SAME),
    (
        'domain_scoped_dot',
        'google.com:my-project.my_dataset.my_table',
        None,
        None, ('google.com:my-project', 'my_dataset', 'my_table'),
        _SAME),
    (
        'partition_decorator',
        'dataset.table$20240101',
        None,
        None, (None, 'dataset', 'table$20240101'),
        _SAME),
    (
        'fq_partition_decorator',
        'project:dataset.table$20240101',
        None,
        None, ('project', 'dataset', 'table$20240101'),
        _SAME),
    (
        'project_ignored_without_dataset',
        'dataset.table',
        None,
        'other-project', (None, 'dataset', 'table'),
        _SAME),
    (
        'dataset_and_table_only',
        'table',
        'dataset',
        None, (None, 'dataset', 'table'),
        _SAME),
    (
        'no_validation_with_dataset',
        'not a valid: table!',
        'd',
        'P', ('P', 'd', 'not a valid: table!'),
        _SAME),
    (
        'mixed_case_dataset_table',
        'project:DataSet.Table',
        None,
        None, ('project', 'DataSet', 'Table'),
        _SAME),
    ('upper_case_project', 'Project:dataset.table', None, None, ERR, _SAME),
    # The project pattern needs at least two characters.
    ('single_char_project', 'p:d.t', None, None, ERR, _SAME),
    ('digit_leading_project', '1project:ds.t', None, None, ERR, _SAME),
    ('dataset_only', 'project:dataset', None, None, ERR, _SAME),
    ('empty', '', None, None, ERR, _SAME),
    ('trailing_dot', 'project:dataset.', None, None, ERR, _SAME),
    ('dash_in_dataset', 'project:data-set.table', None, None, ERR, _SAME),
    # An extra component is absorbed by the domain-scoped project prefix.
    (
        'extra_component',
        'project:dataset.table.extra',
        None,
        None, ('project:dataset', 'table', 'extra'),
        _SAME),
    (
        'unicode_precomposed',
        'project:dataset.t\u00e0ble',
        None,
        None, ('project', 'dataset', 't\u00e0ble'),
        _SAME),
    (
        'unicode_cjk',
        'project:dataset.\u8868\u683c',
        None,
        None, ('project', 'dataset', '\u8868\u683c'),
        _SAME),
    # Documented fallback differences.
    (
        'unicode_combining_mark',
        'project:dataset.ta\u0300ble',
        None,
        None, ('project', 'dataset', 'ta\u0300ble'),
        ERR),
    (
        'unicode_en_dash',
        'project:dataset.a\u2013b',
        None,
        None, ('project', 'dataset', 'a\u2013b'),
        ERR),
    (
        'tab_in_table',
        'project:dataset.a\tb',
        None,
        None,
        ERR, ('project', 'dataset', 'a\tb')),
]


def _check_parse(test, spec, dataset, project, expected):
  if expected is ERR:
    with test.assertRaises(ValueError):
      TableRef.parse(spec, dataset=dataset, project=project)
  else:
    ref = TableRef.parse(spec, dataset=dataset, project=project)
    test.assertEqual((ref.project, ref.dataset_id, ref.table_id), expected)
    test.assertIsInstance(ref, TableRef)


class TableRefParseTest(unittest.TestCase):
  @parameterized.expand(PARSE_CASES)
  @unittest.skipIf(bigquery_resources.regex is None, 'regex is not installed')
  def test_parse_with_regex(self, name, spec, dataset, project, expected, _):
    with mock.patch.object(bigquery_resources,
                           '_TABLE_SPEC_RE',
                           bigquery_resources._compile_patterns(True)):
      _check_parse(self, spec, dataset, project, expected)

  @parameterized.expand(PARSE_CASES)
  def test_parse_with_fallback(
      self, name, spec, dataset, project, expected, fallback):
    expected = expected if fallback is _SAME else fallback
    with mock.patch.object(bigquery_resources,
                           '_TABLE_SPEC_RE',
                           bigquery_resources._compile_patterns(False)):
      _check_parse(self, spec, dataset, project, expected)

  @parameterized.expand(PARSE_CASES)
  @unittest.skipIf(not HAS_MASTER_PARSER, 'apitools or regex is not installed')
  def test_parse_parity_with_master(
      self, name, spec, dataset, project, expected, _):
    # Parity against the live bigquery_tools.parse_table_reference.
    try:
      master = bigquery_tools.parse_table_reference(
          spec, dataset=dataset, project=project)
    except ValueError as e:
      master_error = e
      master = None
    if master is None:
      with self.assertRaises(ValueError) as ctx:
        TableRef.parse(spec, dataset=dataset, project=project)
      self.assertEqual(str(ctx.exception), str(master_error))
      self.assertIs(expected, ERR)
    else:
      ref = TableRef.parse(spec, dataset=dataset, project=project)
      self.assertEqual((master.projectId, master.datasetId, master.tableId),
                       (ref.project, ref.dataset_id, ref.table_id))
      self.assertEqual((ref.project, ref.dataset_id, ref.table_id), expected)

  def test_default_pattern_follows_regex_availability(self):
    if bigquery_resources.regex is None:
      self.assertEqual(
          bigquery_resources._TABLE_SPEC_RE.pattern,
          bigquery_resources._compile_patterns(False).pattern)
    else:
      self.assertEqual(
          bigquery_resources._TABLE_SPEC_RE.pattern,
          bigquery_resources._compile_patterns(True).pattern)

  def test_compile_patterns_without_regex(self):
    with mock.patch.object(bigquery_resources, 'regex', None):
      with self.assertRaises(ImportError):
        bigquery_resources._compile_patterns(True)
      self.assertIsNotNone(bigquery_resources._compile_patterns(False))

  def test_error_message(self):
    with self.assertRaises(ValueError) as ctx:
      TableRef.parse('t')
    self.assertEqual(
        str(ctx.exception),
        'Expected a table reference (PROJECT:DATASET.TABLE or DATASET.TABLE) '
        'instead of t.')

  def test_parse_rejects_non_str(self):
    for value in (None, 1, lambda: 'd.t', StaticValueProvider(str, 'd.t')):
      with self.assertRaises(TypeError):
        TableRef.parse(value)


class ConstructionTest(unittest.TestCase):
  def test_fields(self):
    ref = TableRef('p', 'd', 't')
    self.assertEqual((ref.project, ref.dataset_id, ref.table_id),
                     ('p', 'd', 't'))
    ds = DatasetRef(None, 'd')
    self.assertEqual((ds.project, ds.dataset_id), (None, 'd'))
    job = JobRef('p', 'j')
    self.assertEqual((job.project, job.job_id, job.location), ('p', 'j', None))

  def test_frozen(self):
    for obj, attr in ((TableRef('p', 'd', 't'), 'table_id'),
                      (DatasetRef('p', 'd'), 'project'),
                      (JobRef('p', 'j', 'US'), 'location')):
      with self.assertRaises(dataclasses.FrozenInstanceError):
        setattr(obj, attr, 'x')

  def test_no_slots(self):
    self.assertFalse(hasattr(TableRef, '__slots__'))

  def test_post_init_type_checks(self):
    bad = [
        lambda: TableRef(1, 'd', 't'),
        lambda: TableRef('p', None, 't'),
        lambda: TableRef('p', 'd', None),
        lambda: TableRef(None, 'd', 5),
        lambda: DatasetRef('p', None),
        lambda: DatasetRef(b'p', 'd'),
        lambda: JobRef(None, 'j'),
        lambda: JobRef('p', None),
        lambda: JobRef('p', 'j', 1),
    ]
    for make in bad:
      with self.assertRaises(TypeError):
        make()

  def test_no_format_validation(self):
    TableRef('Not A Project', 'd-d', 't t')
    DatasetRef(None, '')

  def test_equality_and_hash(self):
    a = TableRef('p', 'd', 't')
    b = TableRef('p', 'd', 't')
    self.assertEqual(a, b)
    self.assertEqual(hash(a), hash(b))
    self.assertNotEqual(a, TableRef(None, 'd', 't'))
    self.assertEqual(len({a, b, TableRef('p', 'd', 'u')}), 2)
    self.assertEqual({a: 1}[b], 1)
    self.assertEqual({DatasetRef('p', 'd'), DatasetRef('p', 'd')},
                     {DatasetRef('p', 'd')})
    self.assertEqual({JobRef('p', 'j'): 1}[JobRef('p', 'j')], 1)
    self.assertNotEqual(JobRef('p', 'j'), JobRef('p', 'j', 'US'))


class WithAndResolveTest(unittest.TestCase):
  def test_table_with(self):
    ref = TableRef(None, 'd', 't')
    self.assertEqual(ref.with_project('p'), TableRef('p', 'd', 't'))
    self.assertEqual(ref.with_table_id('u'), TableRef(None, 'd', 'u'))
    self.assertEqual(ref, TableRef(None, 'd', 't'))

  def test_table_resolve(self):
    ref = TableRef(None, 'd', 't')
    self.assertEqual(ref.resolve('p'), TableRef('p', 'd', 't'))
    self.assertIs(ref.resolve(None), ref)
    resolved = TableRef('p', 'd', 't')
    self.assertIs(resolved.resolve('other'), resolved)

  def test_table_dataset(self):
    self.assertEqual(TableRef('p', 'd', 't').dataset(), DatasetRef('p', 'd'))
    self.assertEqual(TableRef(None, 'd', 't').dataset(), DatasetRef(None, 'd'))

  def test_dataset_with_and_resolve(self):
    ds = DatasetRef(None, 'd')
    self.assertEqual(ds.with_project('p'), DatasetRef('p', 'd'))
    self.assertEqual(ds.resolve('p'), DatasetRef('p', 'd'))
    self.assertIs(ds.resolve(None), ds)
    resolved = DatasetRef('p', 'd')
    self.assertIs(resolved.resolve('other'), resolved)

  def test_with_validates(self):
    with self.assertRaises(TypeError):
      TableRef('p', 'd', 't').with_table_id(None)


class FormattingTest(unittest.TestCase):
  def test_to_spec_and_str(self):
    self.assertEqual(TableRef('p', 'd', 't').to_spec(), 'p:d.t')
    self.assertEqual(TableRef(None, 'd', 't').to_spec(), 'd.t')
    self.assertEqual(str(TableRef('p', 'd', 't$1')), 'p:d.t$1')
    self.assertEqual(
        str(TableRef('google.com:p', 'd', 't')), 'google.com:p:d.t')
    self.assertEqual(str(DatasetRef('p', 'd')), 'p:d')
    self.assertEqual(str(DatasetRef(None, 'd')), 'd')

  def test_to_spec_round_trips_through_parse(self):
    for ref in (TableRef('my-project', 'd', 't'),
                TableRef('google.com:my-project', 'd', 't'),
                TableRef(None, 'd', 't$20240101')):
      self.assertEqual(TableRef.parse(ref.to_spec()), ref)

  def test_to_sql(self):
    self.assertEqual(TableRef('p', 'd', 't').to_sql(), '`p.d.t`')
    self.assertEqual(
        TableRef('google.com:p', 'd', 't').to_sql(), '`google.com:p.d.t`')
    self.assertEqual(TableRef('p', 'd', 't').to_sql(legacy=True), '[p:d.t]')
    self.assertEqual(
        TableRef('google.com:p', 'd', 't').to_sql(legacy=True),
        '[google.com:p:d.t]')
    with self.assertRaises(ValueError):
      TableRef(None, 'd', 't').to_sql()
    with self.assertRaises(ValueError):
      TableRef(None, 'd', 't').to_sql(legacy=True)

  def test_to_api_repr(self):
    self.assertEqual(
        TableRef('p', 'd', 't').to_api_repr(), {
            'projectId': 'p', 'datasetId': 'd', 'tableId': 't'
        })
    self.assertEqual(
        TableRef(None, 'd', 't').to_api_repr(), {
            'datasetId': 'd', 'tableId': 't'
        })
    self.assertEqual(
        DatasetRef('p', 'd').to_api_repr(), {
            'projectId': 'p', 'datasetId': 'd'
        })
    self.assertEqual(DatasetRef(None, 'd').to_api_repr(), {'datasetId': 'd'})
    self.assertEqual(
        JobRef('p', 'j').to_api_repr(), {
            'projectId': 'p', 'jobId': 'j'
        })
    self.assertEqual(
        JobRef('p', 'j', 'EU').to_api_repr(), {
            'projectId': 'p', 'jobId': 'j', 'location': 'EU'
        })

  def test_from_api_repr_round_trip(self):
    for ref in (TableRef('p', 'd', 't'),
                TableRef(None, 'd', 't'),
                DatasetRef('p', 'd'),
                DatasetRef(None, 'd'),
                JobRef('p', 'j'),
                JobRef('p', 'j', 'EU')):
      self.assertEqual(type(ref).from_api_repr(ref.to_api_repr()), ref)


class JobRefAliasTest(unittest.TestCase):
  def test_aliases_warn(self):
    job = JobRef('p', 'j', 'US')
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      self.assertEqual(job.jobId, 'j')
    self.assertIn('job_id', str(ctx.warning))
    self.assertEqual(ctx.filename, __file__)
    with self.assertWarns(BeamDeprecationWarning) as ctx:
      self.assertEqual(job.projectId, 'p')
    self.assertIn('project', str(ctx.warning))
    self.assertEqual(ctx.filename, __file__)
    self.assertEqual(job.location, 'US')

  def test_aliases_are_read_only(self):
    job = JobRef('p', 'j')
    with self.assertRaises(dataclasses.FrozenInstanceError):
      job.jobId = 'x'  # pylint: disable=invalid-name
    with self.assertRaises(dataclasses.FrozenInstanceError):
      job.projectId = 'x'  # pylint: disable=invalid-name

  def test_aliases_not_fields(self):
    self.assertEqual([f.name for f in dataclasses.fields(JobRef)],
                     ['project', 'job_id', 'location'])


_SERIALIZATION_VALUES = [
    TableRef('p', 'd', 't'),
    TableRef(None, 'd', 't$20240101'),
    TableRef('google.com:p', 'd', 't'),
    DatasetRef('p', 'd'),
    DatasetRef(None, 'd'),
    JobRef('p', 'j'),
    JobRef('p', 'j', 'EU'),
]


class SerializationTest(unittest.TestCase):
  def test_pickle(self):
    for value in _SERIALIZATION_VALUES:
      for protocol in range(2, pickle.HIGHEST_PROTOCOL + 1):
        self.assertEqual(pickle.loads(pickle.dumps(value, protocol)), value)

  def test_cloudpickle(self):
    for value in _SERIALIZATION_VALUES:
      self.assertEqual(
          cloudpickle_pickler.loads(cloudpickle_pickler.dumps(value)), value)

  @unittest.skipIf(dill_pickler is None, 'dill is not installed')
  def test_dill(self):
    for value in _SERIALIZATION_VALUES:
      self.assertEqual(dill_pickler.loads(dill_pickler.dumps(value)), value)

  def test_beam_pickler(self):
    for value in _SERIALIZATION_VALUES:
      self.assertEqual(pickler.loads(pickler.dumps(value)), value)

  def test_fast_primitives_coder(self):
    coder = coders.FastPrimitivesCoder()
    for value in _SERIALIZATION_VALUES:
      self.assertEqual(coder.decode(coder.encode(value)), value)

  def test_deterministic_coder(self):
    coder = coders.FastPrimitivesCoder().as_deterministic_coder('x')
    for value in _SERIALIZATION_VALUES:
      encoded = coder.encode(value)
      self.assertEqual(coder.decode(encoded), value)
      # Equal refs built separately encode to equal bytes.
      self.assertEqual(encoded, coder.encode(copy.deepcopy(value)))
      self.assertEqual(encoded, coder.encode(dataclasses.replace(value)))
    self.assertNotEqual(
        coder.encode(TableRef('p', 'd', 't')),
        coder.encode(TableRef(None, 'd', 't')))


def _fake(module, name, **attrs):
  """Builds an object whose class has the given module and name."""
  cls = type(name, (), {'__module__': module})
  obj = cls()
  obj.__dict__.update(attrs)
  return obj


class _FakeSchemaField(object):
  __module__ = 'google.cloud.bigquery.schema'

  def __init__(self, api_repr):
    self._api_repr = api_repr

  def to_api_repr(self):
    return self._api_repr


_FakeSchemaField.__name__ = 'SchemaField'
_FakeSchemaField.__qualname__ = 'SchemaField'


class IsDeferredTest(unittest.TestCase):
  def test_is_deferred(self):
    self.assertTrue(is_deferred(StaticValueProvider(str, 'd.t')))
    self.assertTrue(is_deferred(RuntimeValueProvider('opt', str, None)))
    self.assertTrue(is_deferred(lambda: 'd.t'))
    self.assertFalse(is_deferred('d.t'))
    self.assertFalse(is_deferred(None))
    self.assertFalse(is_deferred({'datasetId': 'd', 'tableId': 't'}))
    self.assertFalse(is_deferred(TableRef('p', 'd', 't')))


_DEFERRED = [
    StaticValueProvider(str, 'p:d.t'),
    RuntimeValueProvider('opt', str, None),
    lambda: 'p:d.t',
]


class ToTableRefTest(unittest.TestCase):
  def test_domain_passthrough(self):
    ref = TableRef('p', 'd', 't')
    self.assertIs(to_table_ref(ref), ref)
    self.assertIs(to_table_ref(ref, dataset='x', project='y'), ref)

  def test_str(self):
    self.assertEqual(to_table_ref('proj:d.t'), TableRef('proj', 'd', 't'))
    self.assertEqual(to_table_ref('d.t'), TableRef(None, 'd', 't'))
    self.assertEqual(
        to_table_ref('t', dataset='d', project='p'), TableRef('p', 'd', 't'))
    self.assertEqual(to_table_ref('d.t', project='p'), TableRef(None, 'd', 't'))
    with self.assertRaises(ValueError):
      to_table_ref('t')

  def test_rest_dict(self):
    self.assertEqual(
        to_table_ref({
            'projectId': 'p', 'datasetId': 'd', 'tableId': 't'
        }),
        TableRef('p', 'd', 't'))
    self.assertEqual(
        to_table_ref({
            'datasetId': 'd', 'tableId': 't'
        }, project='ignored'),
        TableRef(None, 'd', 't'))
    self.assertEqual(
        to_table_ref({
            'projectId': None, 'datasetId': 'd', 'tableId': 't'
        }),
        TableRef(None, 'd', 't'))

  def test_rest_dict_errors(self):
    bad_values = [
        {
            'datasetId': 'd'
        },
        {
            'tableId': 't'
        },
        # snake_case keys are not accepted.
        {
            'project': 'p', 'dataset_id': 'd', 'table_id': 't'
        },
        {
            'projectId': 'p', 'datasetId': 'd', 'tableId': 't', 'x': 1
        },
    ]
    for bad in bad_values:
      with self.assertRaises(ValueError):
        to_table_ref(bad)

  def test_deferred(self):
    for value in _DEFERRED:
      with self.assertRaises(TypeError):
        to_table_ref(value)

  def test_unsupported(self):
    for value in (None, 1, DatasetRef('p', 'd'), JobRef('p', 'j'), ['d.t']):
      with self.assertRaises(TypeError):
        to_table_ref(value)

  def test_spoofed_cloud_objects(self):
    for name in ('TableReference', 'Table', 'TableListItem'):
      fake = _fake(
          'google.cloud.bigquery.table',
          name,
          project='p',
          dataset_id='d',
          table_id='t')
      self.assertEqual(
          to_table_ref(fake, dataset='x', project='y'), TableRef('p', 'd', 't'))

  def test_spoofed_wrong_module_or_name(self):
    for module, name in (('some.other.module', 'TableReference'),
                         ('google.cloud.bigquery.table', 'Row')):
      fake = _fake(module, name, project='p', dataset_id='d', table_id='t')
      with self.assertRaises(TypeError):
        to_table_ref(fake)

  @unittest.skipIf(gcp_bigquery is None, 'google-cloud-bigquery not installed')
  def test_real_cloud_objects(self):
    expected = TableRef('p', 'd', 't')
    tr = gcp_bigquery.TableReference.from_string('p.d.t')
    self.assertEqual(to_table_ref(tr), expected)
    self.assertEqual(to_table_ref(gcp_bigquery.Table(tr)), expected)
    item = TableListItem({
        'tableReference': {
            'projectId': 'p', 'datasetId': 'd', 'tableId': 't'
        }
    })
    self.assertEqual(to_table_ref(item), expected)
    self.assertEqual(to_table_ref(tr.to_api_repr()), expected)
    self.assertEqual(
        to_table_ref(
            gcp_bigquery.TableReference.from_string('google.com:p.d.t')),
        TableRef('google.com:p', 'd', 't'))


class ToDatasetRefTest(unittest.TestCase):
  def test_domain_passthrough(self):
    ds = DatasetRef('p', 'd')
    self.assertIs(to_dataset_ref(ds, project='x'), ds)

  def test_str(self):
    self.assertEqual(to_dataset_ref('proj:d'), DatasetRef('proj', 'd'))
    self.assertEqual(to_dataset_ref('proj.d'), DatasetRef('proj', 'd'))
    self.assertEqual(
        to_dataset_ref('google.com:proj:d'), DatasetRef('google.com:proj', 'd'))
    self.assertEqual(
        to_dataset_ref('google.com:proj.d'), DatasetRef('google.com:proj', 'd'))
    self.assertEqual(to_dataset_ref('d'), DatasetRef(None, 'd'))
    self.assertEqual(to_dataset_ref('d', project='p'), DatasetRef('p', 'd'))
    self.assertEqual(
        to_dataset_ref('proj:d', project='other'), DatasetRef('proj', 'd'))
    for bad in ('', 'proj:d.t', 'Proj:d', 'd-d'):
      with self.assertRaises(ValueError):
        to_dataset_ref(bad)

  def test_rest_dict(self):
    self.assertEqual(
        to_dataset_ref({
            'projectId': 'p', 'datasetId': 'd'
        }),
        DatasetRef('p', 'd'))
    self.assertEqual(
        to_dataset_ref({'datasetId': 'd'}, project='ignored'),
        DatasetRef(None, 'd'))
    for bad in ({'projectId': 'p'}, {'project': 'p', 'dataset_id': 'd'}):
      with self.assertRaises(ValueError):
        to_dataset_ref(bad)

  def test_deferred_and_unsupported(self):
    for value in _DEFERRED + [None, 1, TableRef('p', 'd', 't')]:
      with self.assertRaises(TypeError):
        to_dataset_ref(value)

  def test_spoofed_cloud_objects(self):
    for name in ('DatasetReference', 'Dataset', 'DatasetListItem'):
      fake = _fake(
          'google.cloud.bigquery.dataset', name, project='p', dataset_id='d')
      self.assertEqual(to_dataset_ref(fake, project='x'), DatasetRef('p', 'd'))

  @unittest.skipIf(gcp_bigquery is None, 'google-cloud-bigquery not installed')
  def test_real_cloud_objects(self):
    expected = DatasetRef('p', 'd')
    dr = gcp_bigquery.DatasetReference('p', 'd')
    self.assertEqual(to_dataset_ref(dr), expected)
    self.assertEqual(to_dataset_ref(gcp_bigquery.Dataset(dr)), expected)
    item = DatasetListItem(
        {'datasetReference': {
            'projectId': 'p', 'datasetId': 'd'
        }})
    self.assertEqual(to_dataset_ref(item), expected)
    self.assertEqual(to_dataset_ref(dr.to_api_repr()), expected)


class ToJobRefTest(unittest.TestCase):
  def test_domain_passthrough(self):
    job = JobRef('p', 'j')
    self.assertIs(to_job_ref(job), job)

  def test_str_rejected(self):
    with self.assertRaises(TypeError):
      to_job_ref('job_id')

  def test_rest_dict(self):
    self.assertEqual(
        to_job_ref({
            'projectId': 'p', 'jobId': 'j'
        }), JobRef('p', 'j'))
    self.assertEqual(
        to_job_ref({
            'projectId': 'p', 'jobId': 'j', 'location': 'EU'
        }),
        JobRef('p', 'j', 'EU'))
    bad_values = [
        {
            'jobId': 'j'
        },
        {
            'projectId': 'p'
        },
        {
            'project': 'p', 'job_id': 'j'
        },
        {
            'projectId': 'p', 'jobId': 'j', 'state': 'DONE'
        },
    ]
    for bad in bad_values:
      with self.assertRaises(ValueError):
        to_job_ref(bad)

  def test_deferred_and_unsupported(self):
    for value in _DEFERRED + [None, 1, TableRef('p', 'd', 't')]:
      with self.assertRaises(TypeError):
        to_job_ref(value)

  def test_spoofed_cloud_objects(self):
    for module, name in (
        ('google.cloud.bigquery.job.query', 'QueryJob'),
        ('google.cloud.bigquery.job.load', 'LoadJob'),
        ('google.cloud.bigquery.job.copy_', 'CopyJob'),
        ('google.cloud.bigquery.job.extract', 'ExtractJob'),
        ('google.cloud.bigquery.job.base', 'UnknownJob')):
      fake = _fake(module, name, project='p', job_id='j', location='US')
      self.assertEqual(to_job_ref(fake), JobRef('p', 'j', 'US'))

  @unittest.skipIf(gcp_bigquery is None, 'google-cloud-bigquery not installed')
  def test_real_cloud_objects(self):
    client = mock.Mock(project='p')
    jobs = [
        gcp_bigquery.QueryJob('j', 'SELECT 1', client),
        gcp_bigquery.LoadJob(
            'j', ['gs://b/o'],
            gcp_bigquery.TableReference.from_string('p.d.t'),
            client),
        gcp_bigquery.CopyJob(
            'j', [gcp_bigquery.TableReference.from_string('p.d.s')],
            gcp_bigquery.TableReference.from_string('p.d.t'),
            client),
        gcp_bigquery.ExtractJob(
            'j',
            gcp_bigquery.TableReference.from_string('p.d.t'), ['gs://b/o'],
            client),
    ]
    for job in jobs:
      self.assertEqual(to_job_ref(job), JobRef('p', 'j'))
    from google.cloud.bigquery.job.base import _JobReference
    job = gcp_bigquery.QueryJob(
        _JobReference('j', 'p', 'EU'), 'SELECT 1', client)
    self.assertEqual(to_job_ref(job), JobRef('p', 'j', 'EU'))


class ToSchemaDictTest(unittest.TestCase):
  def test_dict_deep_copy(self):
    schema = {
        'fields': [{
            'name': 'r',
            'type': 'RECORD',
            'fields': [{
                'name': 'a', 'type': 'STRING'
            }]
        }]
    }
    original = copy.deepcopy(schema)
    result = to_schema_dict(schema)
    self.assertEqual(result, schema)
    self.assertIsNot(result, schema)
    result['fields'][0]['fields'][0]['name'] = 'changed'
    result['fields'].append({'name': 'x', 'type': 'INT64'})
    self.assertEqual(schema, original)

  def test_dict_not_validated(self):
    self.assertEqual(to_schema_dict({}), {})
    self.assertEqual(to_schema_dict({'x': 1}), {'x': 1})

  def test_str(self):
    self.assertEqual(
        to_schema_dict('a:STRING, b:INTEGER'),
        {
            'fields': [
                {
                    'name': 'a', 'type': 'STRING', 'mode': 'NULLABLE'
                },
                {
                    'name': 'b', 'type': 'INTEGER', 'mode': 'NULLABLE'
                },
            ]
        })

  def test_str_errors(self):
    for bad in ('',
                'a',
                'a:STRING,b',
                'a:b:c',
                '{"fields": [{"name": "a", "type": "STRING"}]}'):
      with self.assertRaises(ValueError):
        to_schema_dict(bad)

  @unittest.skipIf(apitools_bigquery is None, 'apitools is not installed')
  def test_str_parity_with_master(self):
    for schema in ('a:STRING, b:INTEGER',
                   's:STRING',
                   ' a : STRING ,b:RECORD',
                   'a:STRING,,b:INT64',
                   ''):
      try:
        master = bigquery_tools.get_dict_table_schema(schema)
      except ValueError:
        with self.assertRaises(ValueError):
          to_schema_dict(schema)
      else:
        self.assertEqual(to_schema_dict(schema), master)

  def test_spoofed_schema_fields_drop_none(self):
    fields = [
        _FakeSchemaField({
            'name': 'a',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': None,
            'policyTags': None,
        }),
        _FakeSchemaField({
            'name': 'r',
            'type': 'RECORD',
            'mode': 'REPEATED',
            'fields': [{
                'name': 'c', 'type': 'INTEGER', 'maxLength': None
            }],
        }),
    ]
    self.assertEqual(
        to_schema_dict(fields),
        {
            'fields': [
                {
                    'name': 'a', 'type': 'STRING', 'mode': 'NULLABLE'
                },
                {
                    'name': 'r',
                    'type': 'RECORD',
                    'mode': 'REPEATED',
                    'fields': [{
                        'name': 'c', 'type': 'INTEGER'
                    }],
                },
            ]
        })
    self.assertEqual(
        to_schema_dict(tuple(fields)), to_schema_dict(list(fields)))

  def test_schema_fields_result_is_a_copy(self):
    api_repr = {'name': 'a', 'type': 'STRING', 'policyTags': {'names': ['x']}}
    result = to_schema_dict([_FakeSchemaField(api_repr)])
    result['fields'][0]['policyTags']['names'].append('y')
    self.assertEqual(api_repr['policyTags']['names'], ['x'])

  def test_empty_list(self):
    self.assertEqual(to_schema_dict([]), {'fields': []})

  @unittest.skipIf(gcp_bigquery is None, 'google-cloud-bigquery not installed')
  def test_real_schema_fields(self):
    fields = [
        gcp_bigquery.SchemaField('a', 'STRING', mode='REQUIRED'),
        gcp_bigquery.SchemaField(
            'r',
            'RECORD',
            mode='REPEATED',
            description='desc',
            fields=[
                gcp_bigquery.SchemaField('c', 'INTEGER'),
                gcp_bigquery.SchemaField(
                    'n',
                    'RECORD',
                    fields=[
                        gcp_bigquery.SchemaField('x', 'STRING', max_length=10)
                    ]),
            ]),
        gcp_bigquery.SchemaField(
            'p',
            'NUMERIC',
            precision=10,
            scale=2,
            policy_tags=gcp_bigquery.PolicyTagList(names=['tag'])),
    ]
    result = to_schema_dict(fields)
    self.assertEqual(
        result, {'fields': [_drop_nones(f.to_api_repr()) for f in fields]})
    self.assertEqual(result['fields'][0]['mode'], 'REQUIRED')
    self.assertEqual(result['fields'][1]['fields'][1]['fields'][0]['name'], 'x')
    self.assertEqual(result['fields'][2]['policyTags'], {'names': ['tag']})
    # The result converts back to equivalent SchemaFields.
    self.assertEqual(
        [gcp_bigquery.SchemaField.from_api_repr(f) for f in result['fields']],
        fields)

  def test_deferred_and_none(self):
    for value in _DEFERRED + [None]:
      with self.assertRaises(TypeError):
        to_schema_dict(value)

  def test_unsupported(self):
    for value in (1, ['a:STRING'], [{'name': 'a'}], TableRef('p', 'd', 't')):
      with self.assertRaises(TypeError) as ctx:
        to_schema_dict(value)
      self.assertIn('Unexpected schema argument', str(ctx.exception))

  def test_spoofed_mixed_list_rejected(self):
    fake = _FakeSchemaField({'name': 'a', 'type': 'STRING'})
    with self.assertRaises(TypeError):
      to_schema_dict([fake, 'b:STRING'])


def _drop_nones(value):
  if isinstance(value, dict):
    return {k: _drop_nones(v) for k, v in value.items() if v is not None}
  if isinstance(value, list):
    return [_drop_nones(v) for v in value]
  return value


def _drop_none_keys(fields):
  """Drops None-valued keys from master's table_schema_to_dict fields."""
  return [{
      k: (_drop_none_keys(v) if k == 'fields' else v)
      for k, v in f.items() if v is not None
  } for f in fields]


@unittest.skipIf(apitools_bigquery is None, 'apitools is not installed')
class LegacyNormalizerTest(unittest.TestCase):
  """Legacy apitools inputs route through bigquery_compat."""
  def _schema(self):
    tfs = apitools_bigquery.TableFieldSchema
    return apitools_bigquery.TableSchema(
        fields=[
            tfs(name='a', type='STRING', mode='REQUIRED', description='d'),
            tfs(
                name='s',
                type='STRING',
                maxLength=5,
                policyTags=tfs.PolicyTagsValue(names=['tag'])),
            tfs(name='n', type='NUMERIC', precision=10, scale=2),
            tfs(
                name='r',
                type='RECORD',
                mode='REPEATED',
                fields=[
                    tfs(name='c', type='INTEGER'),
                    tfs(
                        name='rr',
                        type='RECORD',
                        fields=[tfs(name='x', type='STRING')]),
                ]),
        ])

  def test_schema_superset_of_master(self):
    schema = self._schema()
    with warnings.catch_warnings():
      warnings.simplefilter('ignore', BeamDeprecationWarning)
      lossless = to_schema_dict(schema)
    master = bigquery_tools.table_schema_to_dict(schema)

    def check(ours, theirs):
      self.assertEqual(len(ours), len(theirs))
      for our_field, their_field in zip(ours, theirs):
        for key, value in their_field.items():
          if key == 'fields':
            check(our_field['fields'], value)
          elif value is None:
            # master emits 'mode': None for unset modes; the REST dict omits
            # the key.
            self.assertNotIn(key, our_field)
          else:
            self.assertEqual(our_field[key], value)

    check(lossless['fields'], master['fields'])
    self.assertEqual(lossless['fields'][1]['policyTags'], {'names': ['tag']})
    self.assertEqual(lossless['fields'][1]['maxLength'], '5')
    self.assertEqual(lossless['fields'][2]['precision'], '10')
    self.assertEqual(lossless['fields'][2]['scale'], '2')
    # Master's lossy output is exactly the lossless output restricted to its
    # keys, once None-valued keys are dropped.
    restricted = {
        'fields': _restrict(
            lossless['fields'], _drop_none_keys(master['fields']))
    }
    self.assertEqual(restricted, {'fields': _drop_none_keys(master['fields'])})

  def test_refs_route_through_bridge(self):
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_table_ref(
              apitools_bigquery.TableReference(
                  projectId='p', datasetId='d', tableId='t'),
              dataset='x',
              project='y'),
          TableRef('p', 'd', 't'))
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_dataset_ref(
              apitools_bigquery.DatasetReference(datasetId='d'), project='y'),
          DatasetRef(None, 'd'))
    with self.assertWarns(BeamDeprecationWarning):
      self.assertEqual(
          to_job_ref(
              apitools_bigquery.JobReference(
                  projectId='p', jobId='j', location='EU')),
          JobRef('p', 'j', 'EU'))

  def test_wrong_legacy_class(self):
    with self.assertRaises(TypeError):
      to_table_ref(apitools_bigquery.DatasetReference(datasetId='d'))
    with self.assertRaises(TypeError):
      to_job_ref(apitools_bigquery.TableReference(datasetId='d', tableId='t'))
    with self.assertRaises(TypeError):
      to_schema_dict(apitools_bigquery.Table())


def _restrict(ours, theirs):
  result = []
  for our_field, their_field in zip(ours, theirs):
    restricted = {}
    for key in their_field:
      if key == 'fields':
        restricted[key] = _restrict(our_field[key], their_field[key])
      else:
        restricted[key] = our_field[key]
    result.append(restricted)
  return result


_HYGIENE_SCRIPT = textwrap.dedent(
    """
    import sys

    # Load what bigquery_resources is allowed to depend on (this also runs
    # the apache_beam package __init__, which may import GCP libraries).
    import apache_beam.options.value_provider
    import apache_beam.utils.annotations

    FORBIDDEN = ('apitools', 'google.cloud.bigquery', 'google.api_core')

    def forbidden(name):
      return any(name == p or name.startswith(p + '.') for p in FORBIDDEN)

    for name in list(sys.modules):
      if forbidden(name):
        del sys.modules[name]
    sys.modules.pop('apache_beam.io.gcp.bigquery_resources', None)

    attempts = []

    class Blocker(object):
      def find_spec(self, name, path=None, target=None):
        if forbidden(name):
          attempts.append(name)
          raise ImportError('blocked: ' + name)
        return None

    sys.meta_path.insert(0, Blocker())

    from apache_beam.io.gcp import bigquery_resources as r
    r.to_table_ref('proj:d.t')
    r.to_table_ref({'datasetId': 'd', 'tableId': 't'})
    r.to_dataset_ref('proj:d')
    r.to_job_ref({'projectId': 'p', 'jobId': 'j'})
    r.to_schema_dict('a:STRING')
    r.to_schema_dict({'fields': []})
    r.TableRef.parse('d.t$1').to_sql
    assert not attempts, attempts
    leaked = [m for m in sys.modules if forbidden(m)]
    assert not leaked, leaked
    print('OK')
    """)


class ImportHygieneTest(unittest.TestCase):
  def test_no_gcp_imports(self):
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(
        p for p in (_SDK_ROOT, env.get('PYTHONPATH')) if p)
    result = subprocess.run([sys.executable, '-c', _HYGIENE_SCRIPT],
                            env=env,
                            capture_output=True,
                            text=True,
                            timeout=300,
                            check=False)
    self.assertEqual(
        result.returncode,
        0,
        'stdout:\n%s\nstderr:\n%s' % (result.stdout, result.stderr))
    self.assertIn('OK', result.stdout)


if __name__ == '__main__':
  unittest.main()
