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

"""Remote service library.

This module contains classes that are useful for building remote services that
conform to a standard request and response model.  To conform to this model
a service must be like the following class:

  # Each service instance only handles a single request and is then discarded.
  # Make these objects light weight.
  class Service(object):

    # It must be possible to construct service objects without any parameters.
    # If your constructor needs extra information you should provide a
    # no-argument factory function to create service instances.
    def __init__(self):
      ...

    # Each remote method must use the 'method' decorator, passing the request
    # and response message types.  The remote method itself must take a single
    # parameter which is an instance of RequestMessage and return an instance
    # of ResponseMessage.
    @method(RequestMessage, ResponseMessage)
    def remote_method(self, request):
      # Return an instance of ResponseMessage.

    # A service object may optionally implement an 'initialize_request_state'
    # method that takes as a parameter a single instance of a RequestState.  If
    # a service does not implement this method it will not receive the request
    # state.
    def initialize_request_state(self, state):
      ...

The 'Service' class is provided as a convenient base class that provides the
above functionality.  It implements all required and optional methods for a
service.  It also has convenience methods for creating factory functions that
can pass persistent global state to a new service instance.

The 'method' decorator is used to declare which methods of a class are
meant to service RPCs.  While this decorator is not responsible for handling
actual remote method invocations, such as handling sockets, handling various
RPC protocols and checking messages for correctness, it does attach information
to methods that responsible classes can examine and ensure the correctness
of the RPC.

When the method decorator is used on a method, the wrapper method will have a
'remote' property associated with it.  The 'remote' property contains the
request_type and response_type expected by the methods implementation.

On its own, the method decorator does not provide any support for subclassing
remote methods.  In order to extend a service, one would need to redecorate
the sub-classes methods.  For example:

  class MyService(Service):

    @method(DoSomethingRequest, DoSomethingResponse)
    def do_stuff(self, request):
      ... implement do_stuff ...

  class MyBetterService(MyService):

    @method(DoSomethingRequest, DoSomethingResponse)
    def do_stuff(self, request):
      response = super(MyBetterService, self).do_stuff.remote.method(request)
      ... do stuff with response ...
      return response

A Service subclass also has a Stub class that can be used with a transport for
making RPCs.  When a stub is created, it is capable of doing both synchronous
and asynchronous RPCs if the underlying transport supports it.  To make a stub
using an HTTP transport do:

  my_service = MyService.Stub(HttpTransport('<my service URL>'))

For synchronous calls, just call the expected methods on the service stub:

  request = DoSomethingRequest()
  ...
  response = my_service.do_something(request)

Each stub instance has an async object that can be used for initiating
asynchronous RPCs if the underlying protocol transport supports it.  To
make an asynchronous call, do:

  rpc = my_service.async.do_something(request)
  response = rpc.get_response()
"""

from __future__ import with_statement
import six

__author__ = 'rafek@google.com (Rafe Kaplan)'

import functools
import logging
import sys
import threading
from wsgiref import headers as wsgi_headers

from . import message_types
from . import messages
from . import protobuf
from . import protojson
from . import util


__all__ = [
    'ApplicationError',
    'MethodNotFoundError',
    'NetworkError',
    'RequestError',
    'RpcError',
    'ServerError',
    'ServiceConfigurationError',
    'ServiceDefinitionError',

    'HttpRequestState',
    'ProtocolConfig',
    'Protocols',
    'RequestState',
    'RpcState',
    'RpcStatus',
    'Service',
    'StubBase',
    'check_rpc_status',
    'get_remote_method_info',
    'is_error_status',
    'method',
    'remote',
]


class ServiceDefinitionError(messages.Error):
  """Raised when a service is improperly defined."""


class ServiceConfigurationError(messages.Error):
  """Raised when a service is incorrectly configured."""


# TODO: Use error_name to map to specific exception message types.
class RpcStatus(messages.Message):
  """Status of on-going or complete RPC.

  Fields:
    state: State of RPC.
    error_name: Error name set by application.  Only set when
      status is APPLICATION_ERROR.  For use by application to transmit
      specific reason for error.
    error_message: Error message associated with status.
  """

  class State(messages.Enum):
    """Enumeration of possible RPC states.

    Values:
      OK: Completed successfully.
      RUNNING: Still running, not complete.
      REQUEST_ERROR: Request was malformed or incomplete.
      SERVER_ERROR: Server experienced an unexpected error.
      NETWORK_ERROR: An error occured on the network.
      APPLICATION_ERROR: The application is indicating an error.
        When in this state, RPC should also set application_error.
    """
    OK = 0
    RUNNING = 1

    REQUEST_ERROR = 2
    SERVER_ERROR = 3
    NETWORK_ERROR = 4
    APPLICATION_ERROR = 5
    METHOD_NOT_FOUND_ERROR = 6

  state = messages.EnumField(State, 1, required=True)
  error_message = messages.StringField(2)
  error_name = messages.StringField(3)


RpcState = RpcStatus.State


class RpcError(messages.Error):
  """Base class for RPC errors.

  Each sub-class of RpcError is associated with an error value from RpcState
  and has an attribute STATE that refers to that value.
  """

  def __init__(self, message, cause=None):
    super(RpcError, self).__init__(message)
    self.cause = cause

  @classmethod
  def from_state(cls, state):
    """Get error class from RpcState.

    Args:
      state: RpcState value.  Can be enum value itself, string or int.

    Returns:
      Exception class mapped to value if state is an error.  Returns None
      if state is OK or RUNNING.
    """
    return _RPC_STATE_TO_ERROR.get(RpcState(state))


class RequestError(RpcError):
  """Raised when wrong request objects received during method invocation."""

  STATE = RpcState.REQUEST_ERROR


class MethodNotFoundError(RequestError):
  """Raised when unknown method requested by RPC."""

  STATE = RpcState.METHOD_NOT_FOUND_ERROR


class NetworkError(RpcError):
  """Raised when network error occurs during RPC."""

  STATE = RpcState.NETWORK_ERROR


class ServerError(RpcError):
  """Unexpected error occured on server."""

  STATE = RpcState.SERVER_ERROR


class ApplicationError(RpcError):
  """Raised for application specific errors.

  Attributes:
    error_name: Application specific error name for exception.
  """

  STATE = RpcState.APPLICATION_ERROR

  def __init__(self, message, error_name=None):
    """Constructor.

    Args:
      message: Application specific error message.
      error_name: Application specific error name.  Must be None, string
      or unicode string.
    """
    super(ApplicationError, self).__init__(message)
    self.error_name = error_name

  def __str__(self):
    return self.args[0]

  def __repr__(self):
    if self.error_name is None:
      error_format = ''
    else:
      error_format = ', %r' % self.error_name
    return '%s(%r%s)' % (type(self).__name__, self.args[0], error_format)


_RPC_STATE_TO_ERROR = {
  RpcState.REQUEST_ERROR: RequestError,
  RpcState.NETWORK_ERROR: NetworkError,
  RpcState.SERVER_ERROR: ServerError,
  RpcState.APPLICATION_ERROR: ApplicationError,
  RpcState.METHOD_NOT_FOUND_ERROR: MethodNotFoundError,
}

class _RemoteMethodInfo(object):
  """Object for encapsulating remote method information.

  An instance of this method is associated with the 'remote' attribute
  of the methods 'invoke_remote_method' instance.

  Instances of this class are created by the remote decorator and should not
  be created directly.
  """

  def __init__(self,
               method,
               request_type,
               response_type):
    """Constructor.

    Args:
      method: The method which implements the remote method.  This is a
        function that will act as an instance method of a class definition
        that is decorated by '@method'.  It must always take 'self' as its
        first parameter.
      request_type: Expected request type for the remote method.
      response_type: Expected response type for the remote method.
    """
    self.__method = method
    self.__request_type = request_type
    self.__response_type = response_type

  @property
  def method(self):
    """Original undecorated method."""
    return self.__method

  @property
  def request_type(self):
    """Expected request type for remote method."""
    if isinstance(self.__request_type, six.string_types):
      self.__request_type = messages.find_definition(
        self.__request_type,
        relative_to=sys.modules[self.__method.__module__])
    return self.__request_type

  @property
  def response_type(self):
    """Expected response type for remote method."""
    if isinstance(self.__response_type, six.string_types):
      self.__response_type = messages.find_definition(
        self.__response_type,
        relative_to=sys.modules[self.__method.__module__])
    return self.__response_type


def method(request_type=message_types.VoidMessage,
           response_type=message_types.VoidMessage):
  """Method decorator for creating remote methods.

  Args:
    request_type: Message type of expected request.
    response_type: Message type of expected response.

  Returns:
    'remote_method_wrapper' function.

  Raises:
    TypeError: if the request_type or response_type parameters are not
      proper subclasses of messages.Message.
  """
  if (not isinstance(request_type, six.string_types) and
      (not isinstance(request_type, type) or
       not issubclass(request_type, messages.Message) or
       request_type is messages.Message)):
    raise TypeError(
        'Must provide message class for request-type.  Found %s',
        request_type)

  if (not isinstance(response_type, six.string_types) and
      (not isinstance(response_type, type) or
       not issubclass(response_type, messages.Message) or
       response_type is messages.Message)):
    raise TypeError(
        'Must provide message class for response-type.  Found %s',
        response_type)

  def remote_method_wrapper(method):
    """Decorator used to wrap method.

    Args:
      method: Original method being wrapped.

    Returns:
      'invoke_remote_method' function responsible for actual invocation.
      This invocation function instance is assigned an attribute 'remote'
      which contains information about the remote method:
        request_type: Expected request type for remote method.
        response_type: Response type returned from remote method.

    Raises:
      TypeError: If request_type or response_type is not a subclass of Message
        or is the Message class itself.
    """

    @functools.wraps(method)
    def invoke_remote_method(service_instance, request):
      """Function used to replace original method.

      Invoke wrapped remote method.  Checks to ensure that request and
      response objects are the correct types.

      Does not check whether messages are initialized.

      Args:
        service_instance: The service object whose method is being invoked.
          This is passed to 'self' during the invocation of the original
          method.
        request: Request message.

      Returns:
        Results of calling wrapped remote method.

      Raises:
        RequestError: Request object is not of the correct type.
        ServerError: Response object is not of the correct type.
      """
      if not isinstance(request, remote_method_info.request_type):
        raise RequestError('Method %s.%s expected request type %s, '
                           'received %s' %
                           (type(service_instance).__name__,
                            method.__name__,
                            remote_method_info.request_type,
                            type(request)))
      response = method(service_instance, request)
      if not isinstance(response, remote_method_info.response_type):
        raise ServerError('Method %s.%s expected response type %s, '
                          'sent %s' %
                          (type(service_instance).__name__,
                           method.__name__,
                           remote_method_info.response_type,
                           type(response)))
      return response

    remote_method_info = _RemoteMethodInfo(method,
                                           request_type,
                                           response_type)

    invoke_remote_method.remote = remote_method_info
    return invoke_remote_method

  return remote_method_wrapper


def remote(request_type, response_type):
  """Temporary backward compatibility alias for method."""
  logging.warning('The remote decorator has been renamed method.  It will be '
                  'removed in very soon from future versions of ProtoRPC.')
  return method(request_type, response_type)


def get_remote_method_info(method):
  """Get remote method info object from remote method.

  Returns:
    Remote method info object if method is a remote method, else None.
  """
  if not callable(method):
    return None

  try:
    method_info = method.remote
  except AttributeError:
    return None

  if not isinstance(method_info, _RemoteMethodInfo):
    return None

  return method_info


class StubBase(object):
  """Base class for client side service stubs.

  The remote method stubs are created by the _ServiceClass meta-class
  when a Service class is first created.  The resulting stub will
  extend both this class and the service class it handles communications for.

  Assume that there is a service:

    class NewContactRequest(messages.Message):

      name = messages.StringField(1, required=True)
      phone = messages.StringField(2)
      email = messages.StringField(3)

    class NewContactResponse(message.Message):

      contact_id = messages.StringField(1)

    class AccountService(remote.Service):

      @remote.method(NewContactRequest, NewContactResponse):
      def new_contact(self, request):
        ... implementation ...

  A stub of this service can be called in two ways.  The first is to pass in a
  correctly initialized NewContactRequest message:

    request = NewContactRequest()
    request.name = 'Bob Somebody'
    request.phone = '+1 415 555 1234'

    response = account_service_stub.new_contact(request)

  The second way is to pass in keyword parameters that correspond with the root
  request message type:

      account_service_stub.new_contact(name='Bob Somebody',
                                       phone='+1 415 555 1234')

  The second form will create a request message of the appropriate type.
  """

  def __init__(self, transport):
    """Constructor.

    Args:
      transport: Underlying transport to communicate with remote service.
    """
    self.__transport = transport

  @property
  def transport(self):
    """Transport used to communicate with remote service."""
    return self.__transport


class _ServiceClass(type):
  """Meta-class for service class."""

  def __new_async_method(cls, remote):
    """Create asynchronous method for Async handler.

    Args:
      remote: RemoteInfo to create method for.
    """
    def async_method(self, *args, **kwargs):
      """Asynchronous remote method.

      Args:
        self: Instance of StubBase.Async subclass.

        Stub methods either take a single positional argument when a full
        request message is passed in, or keyword arguments, but not both.

        See docstring for StubBase for more information on how to use remote
        stub methods.

      Returns:
        Rpc instance used to represent asynchronous RPC.
      """
      if args and kwargs:
        raise TypeError('May not provide both args and kwargs')

      if not args:
        # Construct request object from arguments.
        request = remote.request_type()
        for name, value in six.iteritems(kwargs):
          setattr(request, name, value)
      else:
        # First argument is request object.
        request = args[0]

      return self.transport.send_rpc(remote, request)

    async_method.__name__ = remote.method.__name__
    async_method = util.positional(2)(async_method)
    async_method.remote = remote
    return async_method

  def __new_sync_method(cls, async_method):
    """Create synchronous method for stub.

    Args:
      async_method: asynchronous method to delegate calls to.
    """
    def sync_method(self, *args, **kwargs):
      """Synchronous remote method.

      Args:
        self: Instance of StubBase.Async subclass.
        args: Tuple (request,):
          request: Request object.
        kwargs: Field values for request.  Must be empty if request object
          is provided.

      Returns:
        Response message from synchronized RPC.
      """
      return async_method(self.async, *args, **kwargs).response
    sync_method.__name__ = async_method.__name__
    sync_method.remote = async_method.remote
    return sync_method

  def __create_async_methods(cls, remote_methods):
    """Construct a dictionary of asynchronous methods based on remote methods.

    Args:
      remote_methods: Dictionary of methods with associated RemoteInfo objects.

    Returns:
      Dictionary of asynchronous methods with assocaited RemoteInfo objects.
      Results added to AsyncStub subclass.
    """
    async_methods = {}
    for method_name, method in remote_methods.items():
      async_methods[method_name] = cls.__new_async_method(method.remote)
    return async_methods

  def __create_sync_methods(cls, async_methods):
    """Construct a dictionary of synchronous methods based on remote methods.

    Args:
      async_methods: Dictionary of async methods to delegate calls to.

    Returns:
      Dictionary of synchronous methods with assocaited RemoteInfo objects.
      Results added to Stub subclass.
    """
    sync_methods = {}
    for method_name, async_method in async_methods.items():
      sync_methods[method_name] = cls.__new_sync_method(async_method)
    return sync_methods

  def __new__(cls, name, bases, dct):
    """Instantiate new service class instance."""
    if StubBase not in bases:
      # Collect existing remote methods.
      base_methods = {}
      for base in bases:
        try:
          remote_methods = base.__remote_methods
        except AttributeError:
          pass
        else:
          base_methods.update(remote_methods)

      # Set this class private attribute so that base_methods do not have
      # to be recacluated in __init__.
      dct['_ServiceClass__base_methods'] = base_methods

      for attribute, value in dct.items():
        base_method = base_methods.get(attribute, None)
        if base_method:
          if not callable(value):
            raise ServiceDefinitionError(
              'Must override %s in %s with a method.' % (
                attribute, name))

          if get_remote_method_info(value):
            raise ServiceDefinitionError(
              'Do not use method decorator when overloading remote method %s '
              'on service %s.' %
              (attribute, name))

          base_remote_method_info = get_remote_method_info(base_method)
          remote_decorator = method(
            base_remote_method_info.request_type,
            base_remote_method_info.response_type)
          new_remote_method = remote_decorator(value)
          dct[attribute] = new_remote_method

    return type.__new__(cls, name, bases, dct)

  def __init__(cls, name, bases, dct):
    """Create uninitialized state on new class."""
    type.__init__(cls, name, bases, dct)

    # Only service implementation classes should have remote methods and stub
    # sub classes created.  Stub implementations have their own methods passed
    # in to the type constructor.
    if StubBase not in bases:
      # Create list of remote methods.
      cls.__remote_methods = dict(cls.__base_methods)

      for attribute, value in dct.items():
        value = getattr(cls, attribute)
        remote_method_info = get_remote_method_info(value)
        if remote_method_info:
          cls.__remote_methods[attribute] = value

      # Build asynchronous stub class.
      stub_attributes = {'Service': cls}
      async_methods = cls.__create_async_methods(cls.__remote_methods)
      stub_attributes.update(async_methods)
      async_class = type('AsyncStub', (StubBase, cls), stub_attributes)
      cls.AsyncStub = async_class

      # Constructor for synchronous stub class.
      def __init__(self, transport):
        """Constructor.

        Args:
          transport: Underlying transport to communicate with remote service.
        """
        super(cls.Stub, self).__init__(transport)
        self.async = cls.AsyncStub(transport)

      # Build synchronous stub class.
      stub_attributes = {'Service': cls,
                         '__init__': __init__}
      stub_attributes.update(cls.__create_sync_methods(async_methods))

      cls.Stub = type('Stub', (StubBase, cls), stub_attributes)

  @staticmethod
  def all_remote_methods(cls):
    """Get all remote methods of service.

    Returns:
      Dict from method name to unbound method.
    """
    return dict(cls.__remote_methods)


class RequestState(object):
  """Request state information.

  Properties:
    remote_host: Remote host name where request originated.
    remote_address: IP address where request originated.
    server_host: Host of server within which service resides.
    server_port: Post which service has recevied request from.
  """

  @util.positional(1)
  def __init__(self,
               remote_host=None,
               remote_address=None,
               server_host=None,
               server_port=None):
    """Constructor.

    Args:
      remote_host: Assigned to property.
      remote_address: Assigned to property.
      server_host: Assigned to property.
      server_port: Assigned to property.
    """
    self.__remote_host = remote_host
    self.__remote_address = remote_address
    self.__server_host = server_host
    self.__server_port = server_port

  @property
  def remote_host(self):
    return self.__remote_host

  @property
  def remote_address(self):
    return self.__remote_address

  @property
  def server_host(self):
    return self.__server_host

  @property
  def server_port(self):
    return self.__server_port

  def _repr_items(self):
    for name in ['remote_host',
                 'remote_address',
                 'server_host',
                 'server_port']:
      yield name, getattr(self, name)

  def __repr__(self):
    """String representation of state."""
    state = [self.__class__.__name__]
    for name, value in self._repr_items():
      if value:
        state.append('%s=%r' % (name, value))

    return '<%s>' % (' '.join(state),)


class HttpRequestState(RequestState):
  """HTTP request state information.

  NOTE: Does not attempt to represent certain types of information from the
  request such as the query string as query strings are not permitted in
  ProtoRPC URLs unless required by the underlying message format.

  Properties:
    headers: wsgiref.headers.Headers instance of HTTP request headers.
    http_method: HTTP method as a string.
    service_path: Path on HTTP service where service is mounted.  This path
      will not include the remote method name.
  """

  @util.positional(1)
  def __init__(self,
               http_method=None,
               service_path=None,
               headers=None,
               **kwargs):
    """Constructor.

    Args:
      Same as RequestState, including:
        http_method: Assigned to property.
        service_path: Assigned to property.
        headers: HTTP request headers.  If instance of Headers, assigned to
          property without copying.  If dict, will convert to name value pairs
          for use with Headers constructor.  Otherwise, passed as parameters to
          Headers constructor.
    """
    super(HttpRequestState, self).__init__(**kwargs)

    self.__http_method = http_method
    self.__service_path = service_path

    # Initialize headers.
    if isinstance(headers, dict):
      header_list = []
      for key, value in sorted(headers.items()):
        if not isinstance(value, list):
          value = [value]
        for item in value:
          header_list.append((key, item))
        headers = header_list
    self.__headers = wsgi_headers.Headers(headers or [])

  @property
  def http_method(self):
    return self.__http_method

  @property
  def service_path(self):
    return self.__service_path

  @property
  def headers(self):
    return self.__headers

  def _repr_items(self):
    for item in super(HttpRequestState, self)._repr_items():
      yield item

    for name in ['http_method', 'service_path']:
      yield name, getattr(self, name)

    yield 'headers', list(self.headers.items())


class Service(six.with_metaclass(_ServiceClass, object)):
  """Service base class.

  Base class used for defining remote services.  Contains reflection functions,
  useful helpers and built-in remote methods.

  Services are expected to be constructed via either a constructor or factory
  which takes no parameters.  However, it might be required that some state or
  configuration is passed in to a service across multiple requests.

  To do this, define parameters to the constructor of the service and use
  the 'new_factory' class method to build a constructor that will transmit
  parameters to the constructor.  For example:

    class MyService(Service):

      def __init__(self, configuration, state):
        self.configuration = configuration
        self.state = state

    configuration = MyServiceConfiguration()
    global_state = MyServiceState()

    my_service_factory = MyService.new_factory(configuration,
                                               state=global_state)

  The contract with any service handler is that a new service object is created
  to handle each user request, and that the construction does not take any
  parameters.  The factory satisfies this condition:

    new_instance = my_service_factory()
    assert new_instance.state is global_state

  Attributes:
    request_state: RequestState set via initialize_request_state.
  """

  __request_state = None

  @classmethod
  def all_remote_methods(cls):
    """Get all remote methods for service class.

    Built-in methods do not appear in the dictionary of remote methods.

    Returns:
      Dictionary mapping method name to remote method.
    """
    return _ServiceClass.all_remote_methods(cls)

  @classmethod
  def new_factory(cls, *args, **kwargs):
    """Create factory for service.

    Useful for passing configuration or state objects to the service.  Accepts
    arbitrary parameters and keywords, however, underlying service must accept
    also accept not other parameters in its constructor.

    Args:
      args: Args to pass to service constructor.
      kwargs: Keyword arguments to pass to service constructor.

    Returns:
      Factory function that will create a new instance and forward args and
      keywords to the constructor.
    """

    def service_factory():
      return cls(*args, **kwargs)

    # Update docstring so that it is easier to debug.
    full_class_name = '%s.%s' % (cls.__module__, cls.__name__)
    service_factory.__doc__ = (
        'Creates new instances of service %s.\n\n'
        'Returns:\n'
        '  New instance of %s.'
        % (cls.__name__, full_class_name))

    # Update name so that it is easier to debug the factory function.
    service_factory.__name__ = '%s_service_factory' % cls.__name__

    service_factory.service_class = cls

    return service_factory

  def initialize_request_state(self, request_state):
    """Save request state for use in remote method.

    Args:
      request_state: RequestState instance.
    """
    self.__request_state = request_state

  @classmethod
  def definition_name(cls):
    """Get definition name for Service class.

    Package name is determined by the global 'package' attribute in the
    module that contains the Service definition.  If no 'package' attribute
    is available, uses module name.  If no module is found, just uses class
    name as name.

    Returns:
      Fully qualified service name.
    """
    try:
      return cls.__definition_name
    except AttributeError:
      outer_definition_name = cls.outer_definition_name()
      if outer_definition_name is None:
        cls.__definition_name = cls.__name__
      else:
        cls.__definition_name = '%s.%s' % (outer_definition_name, cls.__name__)

      return cls.__definition_name

  @classmethod
  def outer_definition_name(cls):
    """Get outer definition name.

    Returns:
      Package for service.  Services are never nested inside other definitions.
    """
    return cls.definition_package()

  @classmethod
  def definition_package(cls):
    """Get package for service.

    Returns:
      Package name for service.
    """
    try:
      return cls.__definition_package
    except AttributeError:
      cls.__definition_package = util.get_package_for_module(cls.__module__)

    return cls.__definition_package

  @property
  def request_state(self):
    """Request state associated with this Service instance."""
    return self.__request_state


def is_error_status(status):
  """Function that determines whether the RPC status is an error.

  Args:
    status: Initialized RpcStatus message to check for errors.
  """
  status.check_initialized()
  return RpcError.from_state(status.state) is not None


def check_rpc_status(status):
  """Function converts an error status to a raised exception.

  Args:
    status: Initialized RpcStatus message to check for errors.

  Raises:
    RpcError according to state set on status, if it is an error state.
  """
  status.check_initialized()
  error_class = RpcError.from_state(status.state)
  if error_class is not None:
    if error_class is ApplicationError:
      raise error_class(status.error_message, status.error_name)
    else:
      raise error_class(status.error_message)


class ProtocolConfig(object):
  """Configuration for single protocol mapping.

  A read-only protocol configuration provides a given protocol implementation
  with a name and a set of content-types that it recognizes.

  Properties:
    protocol: The protocol implementation for configuration (usually a module,
      for example, protojson, protobuf, etc.).  This is an object that has the
      following attributes:
        CONTENT_TYPE: Used as the default content-type if default_content_type
          is not set.
        ALTERNATIVE_CONTENT_TYPES (optional): A list of alternative
          content-types to the default that indicate the same protocol.
        encode_message: Function that matches the signature of
          ProtocolConfig.encode_message.  Used for encoding a ProtoRPC message.
        decode_message: Function that matches the signature of
          ProtocolConfig.decode_message.  Used for decoding a ProtoRPC message.
    name: Name of protocol configuration.
    default_content_type: The default content type for the protocol.  Overrides
      CONTENT_TYPE defined on protocol.
    alternative_content_types: A list of alternative content-types supported
      by the protocol.  Must not contain the default content-type, nor
      duplicates.  Overrides ALTERNATIVE_CONTENT_TYPE defined on protocol.
    content_types: A list of all content-types supported by configuration.
      Combination of default content-type and alternatives.
  """

  def __init__(self,
               protocol,
               name,
               default_content_type=None,
               alternative_content_types=None):
    """Constructor.

    Args:
      protocol: The protocol implementation for configuration.
      name: The name of the protocol configuration.
      default_content_type: The default content-type for protocol.  If none
        provided it will check protocol.CONTENT_TYPE.
      alternative_content_types:  A list of content-types.  If none provided,
        it will check protocol.ALTERNATIVE_CONTENT_TYPES.  If that attribute
        does not exist, will be an empty tuple.

    Raises:
      ServiceConfigurationError if there are any duplicate content-types.
    """
    self.__protocol = protocol
    self.__name = name
    self.__default_content_type = (default_content_type or
                                   protocol.CONTENT_TYPE).lower()
    if alternative_content_types is None:
      alternative_content_types = getattr(protocol,
                                          'ALTERNATIVE_CONTENT_TYPES',
                                          ())
    self.__alternative_content_types = tuple(
      content_type.lower() for content_type in alternative_content_types)
    self.__content_types = (
      (self.__default_content_type,) + self.__alternative_content_types)

    # Detect duplicate content types in definition.
    previous_type = None
    for content_type in sorted(self.content_types):
      if content_type == previous_type:
        raise ServiceConfigurationError(
          'Duplicate content-type %s' % content_type)
      previous_type = content_type

  @property
  def protocol(self):
    return self.__protocol

  @property
  def name(self):
    return self.__name

  @property
  def default_content_type(self):
    return self.__default_content_type

  @property
  def alternate_content_types(self):
    return self.__alternative_content_types

  @property
  def content_types(self):
    return self.__content_types

  def encode_message(self, message):
    """Encode message.

    Args:
      message: Message instance to encode.

    Returns:
      String encoding of Message instance encoded in protocol's format.
    """
    return self.__protocol.encode_message(message)

  def decode_message(self, message_type, encoded_message):
    """Decode buffer to Message instance.

    Args:
      message_type: Message type to decode data to.
      encoded_message: Encoded version of message as string.

    Returns:
      Decoded instance of message_type.
    """
    return self.__protocol.decode_message(message_type, encoded_message)


class Protocols(object):
  """Collection of protocol configurations.

  Used to describe a complete set of content-type mappings for multiple
  protocol configurations.

  Properties:
    names: Sorted list of the names of registered protocols.
    content_types: Sorted list of supported content-types.
  """

  __default_protocols = None
  __lock = threading.Lock()

  def __init__(self):
    """Constructor."""
    self.__by_name = {}
    self.__by_content_type = {}

  def add_protocol_config(self, config):
    """Add a protocol configuration to protocol mapping.

    Args:
      config: A ProtocolConfig.

    Raises:
      ServiceConfigurationError if protocol.name is already registered
        or any of it's content-types are already registered.
    """
    if config.name in self.__by_name:
      raise ServiceConfigurationError(
        'Protocol name %r is already in use' % config.name)
    for content_type in config.content_types:
      if content_type in self.__by_content_type:
        raise ServiceConfigurationError(
          'Content type %r is already in use' % content_type)

    self.__by_name[config.name] = config
    self.__by_content_type.update((t, config) for t in config.content_types)

  def add_protocol(self, *args, **kwargs):
    """Add a protocol configuration from basic parameters.

    Simple helper method that creates and registeres a ProtocolConfig instance.
    """
    self.add_protocol_config(ProtocolConfig(*args, **kwargs))

  @property
  def names(self):
    return tuple(sorted(self.__by_name))

  @property
  def content_types(self):
    return tuple(sorted(self.__by_content_type))

  def lookup_by_name(self, name):
    """Look up a ProtocolConfig by name.

    Args:
      name: Name of protocol to look for.

    Returns:
      ProtocolConfig associated with name.

    Raises:
      KeyError if there is no protocol for name.
    """
    return self.__by_name[name.lower()]

  def lookup_by_content_type(self, content_type):
    """Look up a ProtocolConfig by content-type.

    Args:
      content_type: Content-type to find protocol configuration for.

    Returns:
      ProtocolConfig associated with content-type.

    Raises:
      KeyError if there is no protocol for content-type.
    """
    return self.__by_content_type[content_type.lower()]

  @classmethod
  def new_default(cls):
    """Create default protocols configuration.

    Returns:
      New Protocols instance configured for protobuf and protorpc.
    """
    protocols = cls()
    protocols.add_protocol(protobuf, 'protobuf')
    protocols.add_protocol(protojson.ProtoJson.get_default(), 'protojson')
    return protocols

  @classmethod
  def get_default(cls):
    """Get the global default Protocols instance.

    Returns:
      Current global default Protocols instance.
    """
    default_protocols = cls.__default_protocols
    if default_protocols is None:
      with cls.__lock:
        default_protocols = cls.__default_protocols
        if default_protocols is None:
          default_protocols = cls.new_default()
          cls.__default_protocols = default_protocols
    return default_protocols

  @classmethod
  def set_default(cls, protocols):
    """Set the global default Protocols instance.

    Args:
      protocols: A Protocols instance.

    Raises:
      TypeError: If protocols is not an instance of Protocols.
    """
    if not isinstance(protocols, Protocols):
      raise TypeError(
        'Expected value of type "Protocols", found %r' % protocols)
    with cls.__lock:
      cls.__default_protocols = protocols
