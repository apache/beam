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

"""Tests for vcfio module."""

import logging
import os
import shutil
import tempfile
import unittest

import apache_beam.io.source_test_utils as source_test_utils

from apache_beam.io.vcfio import _VcfSource as VcfSource
from apache_beam.io.vcfio import DEFAULT_PHASESET_VALUE
from apache_beam.io.vcfio import MISSING_GENOTYPE_VALUE
from apache_beam.io.vcfio import ReadFromVcf
from apache_beam.io.vcfio import Variant
from apache_beam.io.vcfio import VariantCall
from apache_beam.io.vcfio import VariantInfo

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import BeamAssertException

# Note: mixing \n and \r\n to verify both behaviors.
_SAMPLE_HEADER_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n',
    '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2\r\n',
]


def get_full_file_path(file_name):
  """Returns the full path of the specified ``file_name`` from ``data``."""
  return os.path.join(
      os.path.dirname(__file__), '..', 'testing', 'data', 'vcf', file_name)


def get_full_dir():
  """Returns the full path of the  ``data`` directory."""
  return os.path.join(os.path.dirname(__file__), '..', 'testing', 'data', 'vcf')


# Helper method for comparing variants.
def _variant_comparator(v1, v2):
  if v1.reference_name == v2.reference_name:
    if v1.start == v2.start:
      return cmp(v1.end, v2.end)
    return cmp(v1.start, v2.start)
  return cmp(v1.reference_name, v2.reference_name)


# Helper method for verifying equal count on PCollection.
def _count_equals_to(expected_count):
  def _count_equal(actual_list):
    actual_count = len(actual_list)
    if expected_count != actual_count:
      raise BeamAssertException(
          'Expected %d not equal actual %d' % (expected_count, actual_count))
  return _count_equal


# TODO: Refactor code so all io tests are using same library
# TestCaseWithTempDirCleanup class.
class _TestCaseWithTempDirCleanUp(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result

  def _create_temp_file(self, name='', suffix='', tmpdir=None):
    if not name:
      name = tempfile.template
    return tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=tmpdir or self._new_tempdir(), suffix=suffix)

  def _create_temp_vcf_file(self, lines, tmpdir=None):
    with self._create_temp_file(suffix='.vcf', tmpdir=tmpdir) as f:
      for line in lines:
        f.write(line)
    return f.name


class VcfSourceTest(_TestCaseWithTempDirCleanUp):

  # Distribution should skip tests that need VCF files due to large size
  VCF_FILE_DIR_MISSING = not os.path.exists(get_full_dir())

  def _read_records(self, file_or_pattern):
    source = VcfSource(file_or_pattern)
    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    return read_data

  def _create_temp_file_and_read_records(self, lines):
    return self._read_records(self._create_temp_vcf_file(lines))

  def _assert_variants_equal(self, actual, expected):
    self.assertEqual(
        sorted(expected, cmp=_variant_comparator),
        sorted(actual, cmp=_variant_comparator))

  def _get_sample_variant_1(self):
    vcf_line = ('20	1234	rs123;rs2	C	A,T	50	PASS	AF=0.5,0.1;NS=1	'
                'GT:GQ	0/0:48	1/0:20\n')
    variant = Variant(
        reference_name='20', start=1233, end=1234, reference_bases='C',
        alternate_bases=['A', 'T'], names=['rs123', 'rs2'], quality=50,
        filters=['PASS'],
        info={'AF': VariantInfo(data=[0.5, 0.1], field_count='A'),
              'NS': VariantInfo(data=1, field_count='1')})
    variant.calls.append(
        VariantCall(name='Sample1', genotype=[0, 0], info={'GQ': 48}))
    variant.calls.append(
        VariantCall(name='Sample2', genotype=[1, 0], info={'GQ': 20}))
    return variant, vcf_line

  def _get_sample_variant_2(self):
    vcf_line = (
        '19	123	rs1234	GTC	.	40	q10;s50	NS=2	GT:GQ	1|0:48	0/1:.\n')
    variant = Variant(
        reference_name='19', start=122, end=125, reference_bases='GTC',
        alternate_bases=[], names=['rs1234'], quality=40,
        filters=['q10', 's50'],
        info={'NS': VariantInfo(data=2, field_count='1')})
    variant.calls.append(
        VariantCall(name='Sample1', genotype=[1, 0],
                    phaseset=DEFAULT_PHASESET_VALUE,
                    info={'GQ': 48}))
    variant.calls.append(
        VariantCall(name='Sample2', genotype=[0, 1], info={'GQ': None}))
    return variant, vcf_line

  def _get_sample_variant_3(self):
    vcf_line = (
        '19	12	.	C	<SYMBOLIC>	49	q10	AF=0.5	GT:GQ	0|1:45 .:.\n')
    variant = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['<SYMBOLIC>'], quality=49, filters=['q10'],
        info={'AF': VariantInfo(data=[0.5], field_count='A')})
    variant.calls.append(
        VariantCall(name='Sample1', genotype=[0, 1],
                    phaseset=DEFAULT_PHASESET_VALUE,
                    info={'GQ': 45}))
    variant.calls.append(
        VariantCall(name='Sample2', genotype=[MISSING_GENOTYPE_VALUE],
                    info={'GQ': None}))
    return variant, vcf_line

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_single_file(self):
    test_data_conifgs = [
        {'file': 'valid-4.0.vcf', 'num_records': 5},
        {'file': 'valid-4.0.vcf.gz', 'num_records': 5},
        {'file': 'valid-4.0.vcf.bz2', 'num_records': 5},
        {'file': 'valid-4.1-large.vcf', 'num_records': 9882},
        {'file': 'valid-4.2.vcf', 'num_records': 13},
    ]
    for config in test_data_conifgs:
      read_data = self._read_records(
          get_full_file_path(config['file']))
      self.assertEqual(config['num_records'], len(read_data))

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_file_pattern(self):
    read_data = self._read_records(
        os.path.join(get_full_dir(), 'valid-*.vcf'))
    self.assertEqual(9900, len(read_data))
    read_data_gz = self._read_records(
        os.path.join(get_full_dir(), 'valid-*.vcf.gz'))
    self.assertEqual(9900, len(read_data_gz))

  def test_single_file_no_records(self):
    self.assertEqual(
        [], self._create_temp_file_and_read_records(['']))
    self.assertEqual(
        [], self._create_temp_file_and_read_records(['\n', '\r\n', '\n']))
    self.assertEqual(
        [], self._create_temp_file_and_read_records(_SAMPLE_HEADER_LINES))

  def test_single_file_verify_details(self):
    variant_1, vcf_line_1 = self._get_sample_variant_1()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [vcf_line_1])
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])
    variant_2, vcf_line_2 = self._get_sample_variant_2()
    variant_3, vcf_line_3 = self._get_sample_variant_3()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [vcf_line_1, vcf_line_2, vcf_line_3])
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_file_pattern_verify_details(self):
    variant_1, vcf_line_1 = self._get_sample_variant_1()
    variant_2, vcf_line_2 = self._get_sample_variant_2()
    variant_3, vcf_line_3 = self._get_sample_variant_3()
    tmpdir = self._new_tempdir()
    self._create_temp_vcf_file(_SAMPLE_HEADER_LINES + [vcf_line_1],
                               tmpdir=tmpdir)
    self._create_temp_vcf_file(_SAMPLE_HEADER_LINES + [vcf_line_2, vcf_line_3],
                               tmpdir=tmpdir)
    read_data = self._read_records(os.path.join(tmpdir, '*.vcf'))
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_after_splitting(self):
    file_name = get_full_file_path('valid-4.1-large.vcf')
    source = VcfSource(file_name)
    splits = [split for split in source.split(desired_bundle_size=500)]
    self.assertGreater(len(splits), 1)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    self.assertGreater(len(sources_info), 1)
    split_records = []
    for source_info in sources_info:
      split_records.extend(source_test_utils.read_from_source(*source_info))
    self.assertEqual(9882, len(split_records))

  def test_invalid_file(self):
    invalid_file_contents = [
        # Malfromed record.
        [
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '1    1  '
        ],
        # Missing "GT:GQ" format, but GQ is provided.
        [
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT	1|0:48'
        ],
        # GT is not an integer.
        [
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT	A|0'
        ],
        # Malformed FILTER.
        [
            '##FILTER=<ID=PASS,Description="All filters passed">\n',
            '##FILTER=<ID=LowQual,Descri\n',
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48',
        ],
        # Invalid Number value for INFO.
        [
            '##INFO=<ID=G,Number=U,Type=String,Description="InvalidNumber">\n',
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48\n',
        ],
        # POS should be an integer.
        [
            '##FILTER=<ID=PASS,Description="All filters passed">\n',
            '##FILTER=<ID=LowQual,Descri\n',
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	abc	rs12345	T	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48\n',
        ],
    ]
    for content in invalid_file_contents:
      try:
        self._read_records(self._create_temp_vcf_file(content))
        self.fail('Invalid VCF file must throw an exception')
      except ValueError:
        pass
    # Try with multiple files (any one of them will throw an exception).
    tmpdir = self._new_tempdir()
    for content in invalid_file_contents:
      self._create_temp_vcf_file(content, tmpdir=tmpdir)
    try:
      self._read_records(os.path.join(tmpdir, '*.vcf'))
      self.fail('Invalid VCF file must throw an exception.')
    except ValueError:
      pass

  def test_no_samples(self):
    header_line = '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n'
    record_line = '19	123	.	G	A	.	PASS	AF=0.2'
    expected_variant = Variant(
        reference_name='19', start=122, end=123, reference_bases='G',
        alternate_bases=['A'], filters=['PASS'],
        info={'AF': VariantInfo(data=[0.2], field_count='A')})
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES[:-1] + [header_line, record_line])
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  def test_no_info(self):
    record_line = 'chr19	123	.	.	.	.	.	.	GT	.	.'
    expected_variant = Variant(reference_name='chr19', start=122, end=123)
    expected_variant.calls.append(
        VariantCall(name='Sample1', genotype=[MISSING_GENOTYPE_VALUE]))
    expected_variant.calls.append(
        VariantCall(name='Sample2', genotype=[MISSING_GENOTYPE_VALUE]))
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [record_line])
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  def test_info_numbers_and_types(self):
    info_headers = [
        '##INFO=<ID=HA,Number=A,Type=String,Description="StringInfo_A">\n',
        '##INFO=<ID=HG,Number=G,Type=Integer,Description="IntInfo_G">\n',
        '##INFO=<ID=HR,Number=R,Type=Character,Description="ChrInfo_R">\n',
        '##INFO=<ID=HF,Number=0,Type=Flag,Description="FlagInfo">\n',
        '##INFO=<ID=HU,Number=.,Type=Float,Description="FloatInfo_variable">\n']
    record_lines = [
        '19	2	.	A	T,C	.	.	HA=a1,a2;HG=1,2,3;HR=a,b,c;HF;HU=0.1	GT	1/0	0/1\n',
        '19	124	.	A	T	.	.	HG=3,4,5;HR=d,e;HU=1.1,1.2	GT	0/0	0/1']
    variant_1 = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T', 'C'],
        info={'HA': VariantInfo(data=['a1', 'a2'], field_count='A'),
              'HG': VariantInfo(data=[1, 2, 3], field_count='G'),
              'HR': VariantInfo(data=['a', 'b', 'c'], field_count='R'),
              'HF': VariantInfo(data=True, field_count='0'),
              'HU': VariantInfo(data=[0.1], field_count=None)})
    variant_1.calls.append(VariantCall(name='Sample1', genotype=[1, 0]))
    variant_1.calls.append(VariantCall(name='Sample2', genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=123, end=124, reference_bases='A',
        alternate_bases=['T'],
        info={'HG': VariantInfo(data=[3, 4, 5], field_count='G'),
              'HR': VariantInfo(data=['d', 'e'], field_count='R'),
              'HU': VariantInfo(data=[1.1, 1.2], field_count=None)})
    variant_2.calls.append(VariantCall(name='Sample1', genotype=[0, 0]))
    variant_2.calls.append(VariantCall(name='Sample2', genotype=[0, 1]))
    read_data = self._create_temp_file_and_read_records(
        info_headers + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_end_info_key(self):
    phaseset_header_line = (
        '##INFO=<ID=END,Number=1,Type=Integer,Description="End of record.">\n')
    record_lines = ['19	123	.	A	.	.	.	END=1111	GT	1/0	0/1\n',
                    '19	123	.	A	.	.	.	.	GT	0/1	1/1\n']
    variant_1 = Variant(
        reference_name='19', start=122, end=1111, reference_bases='A')
    variant_1.calls.append(VariantCall(name='Sample1', genotype=[1, 0]))
    variant_1.calls.append(VariantCall(name='Sample2', genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=122, end=123, reference_bases='A')
    variant_2.calls.append(VariantCall(name='Sample1', genotype=[0, 1]))
    variant_2.calls.append(VariantCall(name='Sample2', genotype=[1, 1]))
    read_data = self._create_temp_file_and_read_records(
        [phaseset_header_line] + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_custom_phaseset(self):
    phaseset_header_line = (
        '##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phaseset">\n')
    record_lines = ['19	123	.	A	T	.	.	.	GT:PS	1|0:1111	0/1:.\n',
                    '19	121	.	A	T	.	.	.	GT:PS	1|0:2222	0/1:2222\n']
    variant_1 = Variant(
        reference_name='19', start=122, end=123, reference_bases='A',
        alternate_bases=['T'])
    variant_1.calls.append(
        VariantCall(name='Sample1', genotype=[1, 0], phaseset='1111'))
    variant_1.calls.append(VariantCall(name='Sample2', genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=120, end=121, reference_bases='A',
        alternate_bases=['T'])
    variant_2.calls.append(
        VariantCall(name='Sample1', genotype=[1, 0], phaseset='2222'))
    variant_2.calls.append(
        VariantCall(name='Sample2', genotype=[0, 1], phaseset='2222'))
    read_data = self._create_temp_file_and_read_records(
        [phaseset_header_line] + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_format_numbers(self):
    format_headers = [
        '##FORMAT=<ID=FU,Number=.,Type=String,Description="Format_variable">\n',
        '##FORMAT=<ID=F1,Number=1,Type=Integer,Description="Format_one">\n',
        '##FORMAT=<ID=F2,Number=2,Type=Character,Description="Format_two">\n']
    record_lines = [
        '19	2	.	A	T,C	.	.	.	GT:FU:F1:F2	1/0:a1:3:a,b	0/1:a2,a3:4:b,c\n']
    expected_variant = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T', 'C'])
    expected_variant.calls.append(VariantCall(
        name='Sample1',
        genotype=[1, 0],
        info={'FU': ['a1'], 'F1': 3, 'F2': ['a', 'b']}))
    expected_variant.calls.append(VariantCall(
        name='Sample2',
        genotype=[0, 1],
        info={'FU': ['a2', 'a3'], 'F1': 4, 'F2': ['b', 'c']}))
    read_data = self._create_temp_file_and_read_records(
        format_headers + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_single_file(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromVcf(
        get_full_file_path('valid-4.0.vcf'))
    assert_that(pcoll, _count_equals_to(5))
    pipeline.run()

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_file_pattern(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromVcf(
        os.path.join(get_full_dir(), 'valid-*.vcf'))
    assert_that(pcoll, _count_equals_to(9900))
    pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
