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

"""A source for reading from VCF files (version 4.x).

The 4.2 spec is available at https://samtools.github.io/hts-specs/VCFv4.2.pdf.
"""

from __future__ import absolute_import

import logging
import sys
import traceback
import warnings
from builtins import next
from builtins import object
from collections import namedtuple

from future.utils import iteritems
from past.builtins import long
from past.builtins import unicode

from apache_beam.coders import coders
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.io.textio import _TextSource as TextSource
from apache_beam.transforms import PTransform

if sys.version_info[0] < 3:
  import vcf
else:
  warnings.warn("VCF IO will support Python 3 after migration to Nucleus, "
                "see: BEAM-5628.")


__all__ = ['ReadFromVcf', 'Variant', 'VariantCall', 'VariantInfo',
           'MalformedVcfRecord']

# Stores data about variant INFO fields. The type of 'data' is specified in the
# VCF headers. 'field_count' is a string that specifies the number of fields
# that the data type contains. Its value can either be a number representing a
# constant number of fields, `None` indicating that the value is not set
# (equivalent to '.' in the VCF file) or one of:
#   - 'A': one value per alternate allele.
#   - 'G': one value for each possible genotype.
#   - 'R': one value for each possible allele (including the reference).
VariantInfo = namedtuple('VariantInfo', ['data', 'field_count'])
# Stores data about failed VCF record reads. `line` is the text line that
# caused the failed read and `file_name` is the name of the file that the read
# failed in.
MalformedVcfRecord = namedtuple('MalformedVcfRecord', ['file_name', 'line'])
MISSING_FIELD_VALUE = '.'  # Indicates field is missing in VCF record.
PASS_FILTER = 'PASS'  # Indicates that all filters have been passed.
END_INFO_KEY = 'END'  # The info key that explicitly specifies end of a record.
GENOTYPE_FORMAT_KEY = 'GT'  # The genotype format key in a call.
PHASESET_FORMAT_KEY = 'PS'  # The phaseset format key.
DEFAULT_PHASESET_VALUE = '*'  # Default phaseset value if call is phased, but
                              # no 'PS' is present.
MISSING_GENOTYPE_VALUE = -1  # Genotype to use when '.' is used in GT field.


class Variant(object):
  """A class to store info about a genomic variant.

  Each object corresponds to a single record in a VCF file.
  """
  __hash__ = None

  def __init__(self,
               reference_name=None,
               start=None,
               end=None,
               reference_bases=None,
               alternate_bases=None,
               names=None,
               quality=None,
               filters=None,
               info=None,
               calls=None):
    """Initialize the :class:`Variant` object.

    Args:
      reference_name (str): The reference on which this variant occurs
        (such as `chr20` or `X`). .
      start (int): The position at which this variant occurs (0-based).
        Corresponds to the first base of the string of reference bases.
      end (int): The end position (0-based) of this variant. Corresponds to the
        first base after the last base in the reference allele.
      reference_bases (str): The reference bases for this variant.
      alternate_bases (List[str]): The bases that appear instead of the
        reference bases.
      names (List[str]): Names for the variant, for example a RefSNP ID.
      quality (float): Phred-scaled quality score (-10log10 prob(call is wrong))
        Higher values imply better quality.
      filters (List[str]): A list of filters (normally quality filters) this
        variant has failed. `PASS` indicates this variant has passed all
        filters.
      info (dict): A map of additional variant information. The key is specified
        in the VCF record and the value is of type ``VariantInfo``.
      calls (list of :class:`VariantCall`): The variant calls for this variant.
        Each one represents the determination of genotype with respect to this
        variant.
    """
    self.reference_name = reference_name
    self.start = start
    self.end = end
    self.reference_bases = reference_bases
    self.alternate_bases = alternate_bases or []
    self.names = names or []
    self.quality = quality
    self.filters = filters or []
    self.info = info or {}
    self.calls = calls or []

  def __eq__(self, other):
    return (isinstance(other, Variant) and
            vars(self) == vars(other))

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.reference_name,
                          self.start,
                          self.end,
                          self.reference_bases,
                          self.alternate_bases,
                          self.names,
                          self.quality,
                          self.filters,
                          self.info,
                          self.calls]])

  def __lt__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    # Elements should first be sorted by reference_name, start, end.
    # Ordering of other members is not important, but must be
    # deterministic.
    if self.reference_name != other.reference_name:
      return self.reference_name < other.reference_name
    elif self.start != other.start:
      return self.start < other.start
    elif self.end != other.end:
      return self.end < other.end

    self_vars = vars(self)
    other_vars = vars(other)
    for key in sorted(self_vars):
      if self_vars[key] != other_vars[key]:
        return self_vars[key] < other_vars[key]

    return False

  def __le__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return self < other or self == other

  def __ne__(self, other):
    return not self == other

  def __gt__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return other < self

  def __ge__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return other <= self


class VariantCall(object):
  """A class to store info about a variant call.

  A call represents the determination of genotype with respect to a particular
  variant. It may include associated information such as quality and phasing.
  """

  __hash__ = None

  def __init__(self, name=None, genotype=None, phaseset=None, info=None):
    """Initialize the :class:`VariantCall` object.

    Args:
      name (str): The name of the call.
      genotype (List[int]): The genotype of this variant call as specified by
        the VCF schema. The values are either `0` representing the reference,
        or a 1-based index into alternate bases. Ordering is only important if
        `phaseset` is present. If a genotype is not called (that is, a `.` is
        present in the GT string), -1 is used
      phaseset (str): If this field is present, this variant call's genotype
        ordering implies the phase of the bases and is consistent with any other
        variant calls in the same reference sequence which have the same
        phaseset value. If the genotype data was phased but no phase set was
        specified, this field will be set to `*`.
      info (dict): A map of additional variant call information. The key is
        specified in the VCF record and the type of the value is specified by
        the VCF header FORMAT.
    """
    self.name = name
    self.genotype = genotype or []
    self.phaseset = phaseset
    self.info = info or {}

  def __eq__(self, other):
    return ((self.name, self.genotype, self.phaseset, self.info) ==
            (other.name, other.genotype, other.phaseset, other.info))

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.name, self.genotype, self.phaseset, self.info]])


class _VcfSource(filebasedsource.FileBasedSource):
  """A source for reading VCF files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a uniform PCollection of
  :class:`Variant` objects.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               buffer_size=DEFAULT_VCF_READ_BUFFER_SIZE,
               validate=True,
               allow_malformed_records=False):
    super(_VcfSource, self).__init__(file_pattern,
                                     compression_type=compression_type,
                                     validate=validate)

    self._header_lines_per_file = {}
    self._compression_type = compression_type
    self._buffer_size = buffer_size
    self._allow_malformed_records = allow_malformed_records

  def read_records(self, file_name, range_tracker):
    record_iterator = _VcfSource._VcfRecordIterator(
        file_name,
        range_tracker,
        self._pattern,
        self._compression_type,
        self._allow_malformed_records,
        buffer_size=self._buffer_size,
        skip_header_lines=0)

    # Convert iterator to generator to abstract behavior
    for line in record_iterator:
      yield line

  class _VcfRecordIterator(object):
    """An Iterator for processing a single VCF file."""

    def __init__(self,
                 file_name,
                 range_tracker,
                 file_pattern,
                 compression_type,
                 allow_malformed_records,
                 **kwargs):
      self._header_lines = []
      self._last_record = None
      self._file_name = file_name
      self._allow_malformed_records = allow_malformed_records

      text_source = TextSource(
          file_pattern,
          0,  # min_bundle_size
          compression_type,
          True,  # strip_trailing_newlines
          coders.StrUtf8Coder(),  # coder
          validate=False,
          header_processor_fns=(lambda x: x.startswith('#'),
                                self._store_header_lines),
          **kwargs)

      self._text_lines = text_source.read_records(self._file_name,
                                                  range_tracker)
      try:
        self._vcf_reader = vcf.Reader(fsock=self._create_generator())
      except SyntaxError as e:
        # Throw the exception inside the generator to ensure file is properly
        # closed (it's opened inside TextSource.read_records).
        self._text_lines.throw(
            ValueError('An exception was raised when reading header from VCF '
                       'file %s: %s' % (self._file_name,
                                        traceback.format_exc(e))))

    def _store_header_lines(self, header_lines):
      self._header_lines = header_lines

    def _create_generator(self):
      header_processed = False
      for text_line in self._text_lines:
        if not header_processed and self._header_lines:
          for header in self._header_lines:
            self._last_record = header
            yield self._last_record
          header_processed = True
        # PyVCF has explicit str() calls when parsing INFO fields, which fails
        # with UTF-8 decoded strings. Encode the line back to UTF-8.
        self._last_record = text_line.encode('utf-8')
        yield self._last_record

    def __iter__(self):
      return self

    # pylint: disable=next-method-defined
    def next(self):
      return self.__next__()

    def __next__(self):
      try:
        record = next(self._vcf_reader)
        return self._convert_to_variant_record(record, self._vcf_reader.infos,
                                               self._vcf_reader.formats)
      except (LookupError, ValueError) as e:
        if self._allow_malformed_records:
          logging.warning(
              'An exception was raised when reading record from VCF file '
              '%s. Invalid record was %s: %s',
              self._file_name, self._last_record, traceback.format_exc(e))
          return MalformedVcfRecord(self._file_name, self._last_record)

        # Throw the exception inside the generator to ensure file is properly
        # closed (it's opened inside TextSource.read_records).
        self._text_lines.throw(
            ValueError('An exception was raised when reading record from VCF '
                       'file %s. Invalid record was %s: %s' % (
                           self._file_name,
                           self._last_record,
                           traceback.format_exc(e))))

    def _convert_to_variant_record(self, record, infos, formats):
      """Converts the PyVCF record to a :class:`Variant` object.

      Args:
        record (:class:`~vcf.model._Record`): An object containing info about a
          variant.
        infos (dict): The PyVCF dict storing INFO extracted from the VCF header.
          The key is the info key and the value is :class:`~vcf.parser._Info`.
        formats (dict): The PyVCF dict storing FORMAT extracted from the VCF
          header. The key is the FORMAT key and the value is
          :class:`~vcf.parser._Format`.
      Returns:
        A :class:`Variant` object from the given record.
      """
      variant = Variant()
      variant.reference_name = record.CHROM
      variant.start = record.start
      variant.end = record.end
      variant.reference_bases = (
          record.REF if record.REF != MISSING_FIELD_VALUE else None)
      # ALT fields are classes in PyVCF (e.g. Substitution), so need convert
      # them to their string representations.
      variant.alternate_bases.extend(
          [str(r) for r in record.ALT if r] if record.ALT else [])
      variant.names.extend(record.ID.split(';') if record.ID else [])
      variant.quality = record.QUAL
      # PyVCF uses None for '.' and an empty list for 'PASS'.
      if record.FILTER is not None:
        variant.filters.extend(
            record.FILTER if record.FILTER else [PASS_FILTER])
      for k, v in iteritems(record.INFO):
        # Special case: END info value specifies end of the record, so adjust
        # variant.end and do not include it as part of variant.info.
        if k == END_INFO_KEY:
          variant.end = v
          continue
        field_count = None
        if k in infos:
          field_count = self._get_field_count_as_string(infos[k].num)
        variant.info[k] = VariantInfo(data=v, field_count=field_count)
      for sample in record.samples:
        call = VariantCall()
        call.name = sample.sample
        for allele in sample.gt_alleles or [MISSING_GENOTYPE_VALUE]:
          if allele is None:
            allele = MISSING_GENOTYPE_VALUE
          call.genotype.append(int(allele))
        phaseset_from_format = (getattr(sample.data, PHASESET_FORMAT_KEY)
                                if PHASESET_FORMAT_KEY in sample.data._fields
                                else None)
        # Note: Call is considered phased if it contains the 'PS' key regardless
        # of whether it uses '|'.
        if phaseset_from_format or sample.phased:
          call.phaseset = (str(phaseset_from_format) if phaseset_from_format
                           else DEFAULT_PHASESET_VALUE)
        for field in sample.data._fields:
          # Genotype and phaseset (if present) are already included.
          if field in (GENOTYPE_FORMAT_KEY, PHASESET_FORMAT_KEY):
            continue
          data = getattr(sample.data, field)
          # Convert single values to a list for cases where the number of fields
          # is unknown. This is to ensure consistent types across all records.
          # Note: this is already done for INFO fields in PyVCF.
          if (field in formats and
              formats[field].num is None and
              isinstance(data, (int, float, long, str, unicode, bool))):
            data = [data]
          call.info[field] = data
        variant.calls.append(call)
      return variant

    def _get_field_count_as_string(self, field_count):
      """Returns the string representation of field_count from PyVCF.

      PyVCF converts field counts to an integer with some predefined constants
      as specified in the vcf.parser.field_counts dict (e.g. 'A' is -1). This
      method converts them back to their string representation to avoid having
      direct dependency on the arbitrary PyVCF constants.
      Args:
        field_count (int): An integer representing the number of fields in INFO
          as specified by PyVCF.
      Returns:
        A string representation of field_count (e.g. '-1' becomes 'A').
      Raises:
        ValueError: if the field_count is not valid.
      """
      if field_count is None:
        return None
      elif field_count >= 0:
        return str(field_count)
      field_count_to_string = {v: k for k, v in vcf.parser.field_counts.items()}
      if field_count in field_count_to_string:
        return field_count_to_string[field_count]
      else:
        raise ValueError('Invalid value for field_count: %d' % field_count)


class ReadFromVcf(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading VCF
  files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a PCollection of
  :class:`Variant` (or :class:`MalformedVcfRecord` for failed reads) objects.
  """

  def __init__(
      self,
      file_pattern=None,
      compression_type=CompressionTypes.AUTO,
      validate=True,
      allow_malformed_records=False,
      **kwargs):
    """Initialize the :class:`ReadFromVcf` transform.

    Args:
      file_pattern (str): The file path to read from either as a single file or
        a glob pattern.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
      allow_malformed_records (bool): determines if failed VCF
        record reads will be tolerated. Failed record reads will result in a
        :class:`MalformedVcfRecord` being returned from the read of the record
        rather than a :class:`Variant`.
    """
    super(ReadFromVcf, self).__init__(**kwargs)
    self._source = _VcfSource(
        file_pattern,
        compression_type,
        validate=validate,
        allow_malformed_records=allow_malformed_records)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)
