#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""URL encoding support for messages types.

Protocol support for URL encoded form parameters.

Nested Fields:
  Nested fields are repesented by dot separated names.  For example, consider
  the following messages:

    class WebPage(Message):

      title = StringField(1)
      tags = StringField(2, repeated=True)

    class WebSite(Message):

      name = StringField(1)
      home = MessageField(WebPage, 2)
      pages = MessageField(WebPage, 3, repeated=True)

  And consider the object:

    page = WebPage()
    page.title = 'Welcome to NewSite 2010'

    site = WebSite()
    site.name = 'NewSite 2010'
    site.home = page

  The URL encoded representation of this constellation of objects is.

    name=NewSite+2010&home.title=Welcome+to+NewSite+2010

  An object that exists but does not have any state can be represented with
  a reference to its name alone with no value assigned to it.  For example:

    page = WebSite()
    page.name = 'My Empty Site'
    page.home = WebPage()

  is represented as:

    name=My+Empty+Site&home=

  This represents a site with an empty uninitialized home page.

Repeated Fields:
  Repeated fields are represented by the name of and the index of each value
  separated by a dash.  For example, consider the following message:

    home = Page()
    home.title = 'Nome'

    news = Page()
    news.title = 'News'
    news.tags = ['news', 'articles']

    instance = WebSite()
    instance.name = 'Super fun site'
    instance.pages = [home, news, preferences]

  An instance of this message can be represented as:

    name=Super+fun+site&page-0.title=Home&pages-1.title=News&...
    pages-1.tags-0=new&pages-1.tags-1=articles

Helper classes:

  URLEncodedRequestBuilder: Used for encapsulating the logic used for building
    a request message from a URL encoded RPC.
"""
import six

__author__ = 'rafek@google.com (Rafe Kaplan)'

import cgi
import re
import urllib

from . import message_types
from . import messages
from . import util

__all__ = ['CONTENT_TYPE',
           'URLEncodedRequestBuilder',
           'encode_message',
           'decode_message',
           ]

CONTENT_TYPE = 'application/x-www-form-urlencoded'

_FIELD_NAME_REGEX = re.compile(r'^([a-zA-Z_][a-zA-Z_0-9]*)(?:-([0-9]+))?$')


class URLEncodedRequestBuilder(object):
  """Helper that encapsulates the logic used for building URL encoded messages.

  This helper is used to map query parameters from a URL encoded RPC to a
  message instance.
  """

  @util.positional(2)
  def __init__(self, message, prefix=''):
    """Constructor.

    Args:
      message: Message instance to build from parameters.
      prefix: Prefix expected at the start of valid parameters.
    """
    self.__parameter_prefix = prefix

    # The empty tuple indicates the root message, which has no path.
    # __messages is a full cache that makes it very easy to look up message
    # instances by their paths.  See make_path for details about what a path
    # is.
    self.__messages = {(): message}

    # This is a cache that stores paths which have been checked for
    # correctness.  Correctness means that an index is present for repeated
    # fields on the path and absent for non-repeated fields.  The cache is
    # also used to check that indexes are added in the right order so that
    # dicontiguous ranges of indexes are ignored.
    self.__checked_indexes = set([()])

  def make_path(self, parameter_name):
    """Parse a parameter name and build a full path to a message value.

    The path of a method is a tuple of 2-tuples describing the names and
    indexes within repeated fields from the root message (the message being
    constructed by the builder) to an arbitrarily nested message within it.

    Each 2-tuple node of a path (name, index) is:
      name: The name of the field that refers to the message instance.
      index: The index within a repeated field that refers to the message
        instance, None if not a repeated field.

    For example, consider:

      class VeryInner(messages.Message):
        ...

      class Inner(messages.Message):

        very_inner = messages.MessageField(VeryInner, 1, repeated=True)

      class Outer(messages.Message):

        inner = messages.MessageField(Inner, 1)

    If this builder is building an instance of Outer, that instance is
    referred to in the URL encoded parameters without a path.  Therefore
    its path is ().

    The child 'inner' is referred to by its path (('inner', None)).

    The first child of repeated field 'very_inner' on the Inner instance
    is referred to by (('inner', None), ('very_inner', 0)).

    Examples:
      # Correct reference to model where nation is a Message, district is
      # repeated Message and county is any not repeated field type.
      >>> make_path('nation.district-2.county')
      (('nation', None), ('district', 2), ('county', None))

      # Field is not part of model.
      >>> make_path('nation.made_up_field')
      None

      # nation field is not repeated and index provided.
      >>> make_path('nation-1')
      None

      # district field is repeated and no index provided.
      >>> make_path('nation.district')
      None

    Args:
      parameter_name: Name of query parameter as passed in from the request.
        in order to make a path, this parameter_name must point to a valid
        field within the message structure.  Nodes of the path that refer to
        repeated fields must be indexed with a number, non repeated nodes must
        not have an index.

    Returns:
      Parsed version of the parameter_name as a tuple of tuples:
        attribute: Name of attribute associated with path.
        index: Postitive integer index when it is a repeated field, else None.
      Will return None if the parameter_name does not have the right prefix,
      does not point to a field within the message structure, does not have
      an index if it is a repeated field or has an index but is not a repeated
      field.
    """
    if parameter_name.startswith(self.__parameter_prefix):
      parameter_name = parameter_name[len(self.__parameter_prefix):]
    else:
      return None

    path = []
    name = []
    message_type = type(self.__messages[()])  # Get root message.

    for item in parameter_name.split('.'):
      # This will catch sub_message.real_message_field.not_real_field
      if not message_type:
        return None

      item_match = _FIELD_NAME_REGEX.match(item)
      if not item_match:
        return None
      attribute = item_match.group(1)
      index = item_match.group(2)
      if index:
        index = int(index)

      try:
        field = message_type.field_by_name(attribute)
      except KeyError:
        return None

      if field.repeated != (index is not None):
        return None

      if isinstance(field, messages.MessageField):
        message_type = field.message_type
      else:
        message_type = None

      # Path is valid so far.  Append node and continue.
      path.append((attribute, index))

    return tuple(path)

  def __check_index(self, parent_path, name, index):
    """Check correct index use and value relative to a given path.

    Check that for a given path the index is present for repeated fields
    and that it is in range for the existing list that it will be inserted
    in to or appended to.

    Args:
      parent_path: Path to check against name and index.
      name: Name of field to check for existance.
      index: Index to check.  If field is repeated, should be a number within
        range of the length of the field, or point to the next item for
        appending.
    """
    # Don't worry about non-repeated fields.
    # It's also ok if index is 0 because that means next insert will append.
    if not index:
      return True

    parent = self.__messages.get(parent_path, None)
    value_list = getattr(parent, name, None)
    # If the list does not exist then the index should be 0.  Since it is
    # not, path is not valid.
    if not value_list:
      return False

    # The index must either point to an element of the list or to the tail.
    return len(value_list) >= index

  def __check_indexes(self, path):
    """Check that all indexes are valid and in the right order.

    This method must iterate over the path and check that all references
    to indexes point to an existing message or to the end of the list, meaning
    the next value should be appended to the repeated field.

    Args:
      path: Path to check indexes for.  Tuple of 2-tuples (name, index).  See
        make_path for more information.

    Returns:
      True if all the indexes of the path are within range, else False.
    """
    if path in self.__checked_indexes:
      return True

    # Start with the root message.
    parent_path = ()

    for name, index in path:
      next_path = parent_path + ((name, index),)
      # First look in the checked indexes cache.
      if next_path not in self.__checked_indexes:
        if not self.__check_index(parent_path, name, index):
          return False
        self.__checked_indexes.add(next_path)

      parent_path = next_path

    return True

  def __get_or_create_path(self, path):
    """Get a message from the messages cache or create it and add it.

    This method will also create any parent messages based on the path.

    When a new instance of a given message is created, it is stored in
    __message by its path.

    Args:
      path: Path of message to get.  Path must be valid, in other words
        __check_index(path) returns true.  Tuple of 2-tuples (name, index).
        See make_path for more information.

    Returns:
      Message instance if the field being pointed to by the path is a
      message, else will return None for non-message fields.
    """
    message = self.__messages.get(path, None)
    if message:
      return message

    parent_path = ()
    parent = self.__messages[()]  # Get the root object

    for name, index in path:
      field = parent.field_by_name(name)
      next_path = parent_path + ((name, index),)
      next_message = self.__messages.get(next_path, None)
      if next_message is None:
        next_message = field.message_type()
        self.__messages[next_path] = next_message
        if not field.repeated:
          setattr(parent, field.name, next_message)
        else:
          list_value = getattr(parent, field.name, None)
          if list_value is None:
            setattr(parent, field.name, [next_message])
          else:
            list_value.append(next_message)

      parent_path = next_path
      parent = next_message

    return parent

  def add_parameter(self, parameter, values):
    """Add a single parameter.

    Adds a single parameter and its value to the request message.

    Args:
      parameter: Query string parameter to map to request.
      values: List of values to assign to request message.

    Returns:
      True if parameter was valid and added to the message, else False.

    Raises:
      DecodeError if the parameter refers to a valid field, and the values
      parameter does not have one and only one value.  Non-valid query
      parameters may have multiple values and should not cause an error.
    """
    path = self.make_path(parameter)

    if not path:
      return False

    # Must check that all indexes of all items in the path are correct before
    # instantiating any of them.  For example, consider:
    #
    #   class Repeated(object):
    #     ...
    #
    #   class Inner(object):
    #
    #     repeated = messages.MessageField(Repeated, 1, repeated=True)
    #
    #   class Outer(object):
    #
    #     inner = messages.MessageField(Inner, 1)
    #
    #   instance = Outer()
    #   builder = URLEncodedRequestBuilder(instance)
    #   builder.add_parameter('inner.repeated')
    #
    #   assert not hasattr(instance, 'inner')
    #
    # The check is done relative to the instance of Outer pass in to the
    # constructor of the builder.  This instance is not referred to at all
    # because all names are assumed to be relative to it.
    #
    # The 'repeated' part of the path is not correct because it is missing an
    # index.  Because it is missing an index, it should not create an instance
    # of Repeated.  In this case add_parameter will return False and have no
    # side effects.
    #
    # A correct path that would cause a new Inner instance to be inserted at
    # instance.inner and a new Repeated instance to be appended to the
    # instance.inner.repeated list would be 'inner.repeated-0'.
    if not self.__check_indexes(path):
      return False

    # Ok to build objects.
    parent_path = path[:-1]
    parent = self.__get_or_create_path(parent_path)
    name, index = path[-1]
    field = parent.field_by_name(name)

    if len(values) != 1:
      raise messages.DecodeError(
          'Found repeated values for field %s.' % field.name)

    value = values[0]

    if isinstance(field, messages.IntegerField):
      converted_value = int(value)
    elif isinstance(field, message_types.DateTimeField):
      try:
        converted_value = util.decode_datetime(value)
      except ValueError as e:
        raise messages.DecodeError(e)
    elif isinstance(field, messages.MessageField):
      # Just make sure it's instantiated.  Assignment to field or
      # appending to list is done in __get_or_create_path.
      self.__get_or_create_path(path)
      return True
    elif isinstance(field, messages.StringField):
      converted_value = value.decode('utf-8')
    elif isinstance(field, messages.BooleanField):
      converted_value = value.lower() == 'true' and True or False
    else:
      try:
        converted_value = field.type(value)
      except TypeError:
        raise messages.DecodeError('Invalid enum value "%s"' % value)

    if field.repeated:
      value_list = getattr(parent, field.name, None)
      if value_list is None:
        setattr(parent, field.name, [converted_value])
      else:
        if index == len(value_list):
          value_list.append(converted_value)
        else:
          # Index should never be above len(value_list) because it was
          # verified during the index check above.
          value_list[index] = converted_value
    else:
      setattr(parent, field.name, converted_value)

    return True


@util.positional(1)
def encode_message(message, prefix=''):
  """Encode Message instance to url-encoded string.

  Args:
    message: Message instance to encode in to url-encoded string.
    prefix: Prefix to append to field names of contained values.

  Returns:
    String encoding of Message in URL encoded format.

  Raises:
    messages.ValidationError if message is not initialized.
  """
  message.check_initialized()

  parameters = []
  def build_message(parent, prefix):
    """Recursively build parameter list for URL response.

    Args:
      parent: Message to build parameters for.
      prefix: Prefix to append to field names of contained values.

    Returns:
      True if some value of parent was added to the parameters list,
      else False, meaning the object contained no values.
    """
    has_any_values = False
    for field in sorted(parent.all_fields(), key=lambda f: f.number):
      next_value = parent.get_assigned_value(field.name)
      if next_value is None:
        continue

      # Found a value.  Ultimate return value should be True.
      has_any_values = True

      # Normalize all values in to a list.
      if not field.repeated:
        next_value = [next_value]

      for index, item in enumerate(next_value):
        # Create a name with an index if it is a repeated field.
        if field.repeated:
          field_name = '%s%s-%s' % (prefix, field.name, index)
        else:
          field_name = prefix + field.name

        if isinstance(field, message_types.DateTimeField):
          # DateTimeField stores its data as a RFC 3339 compliant string.
          parameters.append((field_name, item.isoformat()))
        elif isinstance(field, messages.MessageField):
          # Message fields must be recursed in to in order to construct
          # their component parameter values.
          if not build_message(item, field_name + '.'):
            # The nested message is empty.  Append an empty value to
            # represent it.
            parameters.append((field_name, ''))
        elif isinstance(field, messages.BooleanField):
          parameters.append((field_name, item and 'true' or 'false'))
        else:
          if isinstance(item, six.text_type):
            item = item.encode('utf-8')
          parameters.append((field_name, str(item)))

    return has_any_values

  build_message(message, prefix)

  # Also add any unrecognized values from the decoded string.
  for key in message.all_unrecognized_fields():
    values, _ = message.get_unrecognized_field_info(key)
    if not isinstance(values, (list, tuple)):
      values = (values,)
    for value in values:
      parameters.append((key, value))

  return urllib.urlencode(parameters)


def decode_message(message_type, encoded_message, **kwargs):
  """Decode urlencoded content to message.

  Args:
    message_type: Message instance to merge URL encoded content into.
    encoded_message: URL encoded message.
    prefix: Prefix to append to field names of contained values.

  Returns:
    Decoded instance of message_type.
  """
  message = message_type()
  builder = URLEncodedRequestBuilder(message, **kwargs)
  arguments = cgi.parse_qs(encoded_message, keep_blank_values=True)
  for argument, values in sorted(six.iteritems(arguments)):
    added = builder.add_parameter(argument, values)
    # Save off any unknown values, so they're still accessible.
    if not added:
      message.set_unrecognized_field(argument, values, messages.Variant.STRING)
  message.check_initialized()
  return message
