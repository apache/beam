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

"""hypothesis strategies for generating schema types.

Intended for internal use only, no backward-compatibility guarantees."""

import keyword
import unicodedata
from typing import Mapping
from typing import Optional
from typing import Sequence

from hypothesis import strategies as st

from apache_beam.typehints import row_type
from apache_beam.typehints.schemas import _PRIMITIVES

PRIMITIVES = [p[0] for p in _PRIMITIVES]


def field_names():
  @st.composite
  def field_name_candidates(draw):
    """Strategy to produce valid field names for Beam schema types."""
    # unicode categories that cannot be used in Python identifiers
    identifer_denylist = (
        'Lo', 'Lm', 'C', 'P', 'Sm', 'Sc', 'So', 'Sk', 'M', 'No', 'Z')

    # First character can't be numeric (Nd).
    # It also can't start with '_' in a NamedTuple.
    field_first_character = draw(
        st.text(
            alphabet=st.characters(
                blacklist_categories=('Nd', ) + identifer_denylist,
                blacklist_characters=('_')),
            min_size=1,
            max_size=1))
    field_remainder = draw(
        st.text(
            alphabet=st.characters(blacklist_categories=identifer_denylist)))

    return field_first_character + field_remainder

  return field_name_candidates().filter(
      lambda s: s.isidentifier() and not keyword.iskeyword(s))


def _named_fields_from_types(types):
  return st.lists(
      st.tuples(field_names(), types),
      min_size=1,
      # Python identifiers are normalized with form NFKC (see
      # https://peps.python.org/pep-0672/#normalizing-identifiers). We use the
      # same normalization here to avoid name collisions.
      unique_by=lambda name_and_type: unicodedata.normalize(
          'NFKC', name_and_type[0]),
  )


def types():
  """Strategy to produce types that are convertible to Beam schema FieldType
  instances."""
  def _extend_types(types):
    optionals = types.map(lambda typ: Optional[typ])
    sequences = types.map(lambda typ: Sequence[typ])
    mappings = st.tuples(types,
                         types).map(lambda typs: Mapping[typs[0], typs[1]])
    rows = _named_fields_from_types(types).map(
        row_type.RowTypeConstraint.from_fields)

    return st.one_of(optionals, sequences, mappings, rows)

  # TODO: Currently this will only draw from the primitive types that can be
  # roundtripped faithfully (e.g. np.int64, not int). We should add support for
  # other types:
  # - Logical Types (e.g. Timestamp)
  # - Shunted primitive types (e.g. int)
  # We'll need to provide support for limiting the types that are drawn. This
  # could be similar to the allowlist[_categories]/denylist[_categories] pattern
  # used in st.characters.
  return st.recursive(st.sampled_from(PRIMITIVES), _extend_types)


def named_fields():
  """Strategy to produce a set of named fields (type ``List[Tuple[str,
  type]]``)."""
  return _named_fields_from_types(types())
