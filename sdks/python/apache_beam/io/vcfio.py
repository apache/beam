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

from collections import namedtuple

import vcf

from apache_beam.coders import coders
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.io.textio import _TextSource as TextSource
from apache_beam.transforms import PTransform

__all__ = ['ReadFromVcf', 'Variant', 'VariantCall', 'VariantInfo']


# Stores data about variant INFO fields. The type of `data` is specified in the
# VCF headers. `field_count` is a string that specifies the number of fields
# that the data type contains. Its value can either be a number representing a
# constant number of fields, `None` indicating that the value is not set
# (equivalent to `.` in the VCF file) or one of:
#   - `A`: one value per alternate allele.
#   - `G`: one value for each possible genotype.
#   - `R`: one value for each possible allele (including the reference).
VariantInfo = namedtuple('VariantInfo', ['data', 'field_count'])
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
    return (self.reference_name == other.reference_name and
            self.start == other.start and
            self.end == other.end and
            self.reference_bases == other.reference_bases and
            self.alternate_bases == other.alternate_bases and
            self.names == other.names and
            self.quality == other.quality and
            self.filters == other.filters and
            self.info == other.info and
            self.calls == other.calls)

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


class VariantCall(object):
  """A class to store info about a variant call.

  A call represents the determination of genotype with respect to a particular
  variant. It may include associated information such as quality and phasing.
  """

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
    return (self.name == other.name and
            self.genotype == other.genotype and
            self.phaseset == other.phaseset and
            self.info == other.info)

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.name, self.genotype, self.phaseset, self.info]])


class _VcfSource(filebasedsource.FileBasedSource):
  r"""A source for reading VCF files.

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
               validate=True):
    super(_VcfSource, self).__init__(file_pattern,
                                     compression_type=compression_type,
                                     validate=validate)

    self._header_lines_per_file = {}
    self._file_pattern = file_pattern
    self._compression_type = compression_type
    self._buffer_size = buffer_size
    self._validate = validate

  def read_records(self, file_name, range_tracker):
    def line_generator():
      header_lines = []

      def store_header_lines(header):
        for line in header:
          header_lines.append(line)

      text_source = TextSource(
          self._file_pattern,
          0,  # min_bundle_size
          self._compression_type,
          strip_trailing_newlines=True,
          coder=coders.StrUtf8Coder(),
          buffer_size=self._buffer_size,
          validate=self._validate,
          skip_header_lines=0,
          header_matcher_predicate=lambda x: x.startswith('#'),
          header_processor=store_header_lines)

      records = text_source.read_records(file_name, range_tracker)
      header_processed = False
      for line in records:
        if not header_processed and header_lines:
          for header in header_lines:
            yield header
          header_processed = True
        # PyVCF has explicit str() calls when parsing INFO fields, which fails
        # with UTF-8 decoded strings. Encode the line back to UTF-8.
        yield line.encode('utf-8')

    try:
      vcf_reader = vcf.Reader(fsock=line_generator())
      while True:
        record = next(vcf_reader)
        yield self._convert_to_variant_record(
            record, vcf_reader.infos, vcf_reader.formats)
    except SyntaxError as e:
      raise ValueError('Invalid VCF header: %s' % str(e))
    except (LookupError, ValueError) as e:
      # TODO: Add 'strict' and 'loose' modes to not throw an
      # exception in case of such failures.
      raise ValueError('Invalid record in VCF file. Error: %s' % str(e))

  def _convert_to_variant_record(self, record, infos, formats):
    """Converts the PyVCF record to a :class:`Variant` object.

    Args:
      record (:class:`~vcf.model._Record`): An object containing info about a
        variant.
      infos (dict): The PyVCF dict storing INFO extracted from the VCF header.
        The key is the info key and the value is :class:`~vcf.parser._Info`.
      fromats (dict): The PyVCF dict storing FORMAT extracted from the VCF
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
    # ALT fields are classes in PyVCF (e.g. Substitution), so need convert them
    # to their string representations.
    variant.alternate_bases.extend(
        [str(r) for r in record.ALT if r] if record.ALT else [])
    variant.names.extend(record.ID.split(';') if record.ID else [])
    variant.quality = record.QUAL
    # PyVCF uses None for '.' and an empty list for 'PASS'.
    if record.FILTER is not None:
      variant.filters.extend(record.FILTER if record.FILTER else [PASS_FILTER])
    for k, v in record.INFO.iteritems():
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
        if (field in formats and formats[field].num is None and
            isinstance(data, (int, float, long, basestring, bool))):
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
  :class:`Variant` objects.
  """

  def __init__(
      self,
      file_pattern=None,
      compression_type=CompressionTypes.AUTO,
      validate=True,
      **kwargs):
    """Initialize the :class:`ReadFromVcf` transform.

    Args:
      file_pattern (str): The file path to read from as a local file path.
        The path can contain glob characters (``*``, ``?``, and ``[...]`` sets).
      min_bundle_size (int): Minimum size of bundles that should be generated
        when splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
    """
    super(ReadFromVcf, self).__init__(**kwargs)
    self._source = _VcfSource(
        file_pattern, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)
