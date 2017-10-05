

import cPickle as pickle
import json
import logging
import multiprocessing
import random
import socket
import struct
import subprocess
import sys
import threading
import time

## START RPC layer stuff

class HostDescriptor(object):
  def __init__(self, host_id):
    self.host_id = host_id

  def serialize(self):
    return host_id

  @classmethod
  def generate(cls):
    # guaranteed to be random
    return cls(random.randint(1, 100000))

  def __repr__(self):
    return 'HostDescriptor[%d]' % self.host_id


def get_remote_interface(host_descriptor, channel_manager, interface_name, interface_cls):
  class RemoteInterface(interface_cls):
    pass
  interface_obj = RemoteInterface()
  # print interface_cls, 'dict', dir(interface_obj), interface_obj.ping
  for name in dir(interface_obj):
    prop = getattr(interface_obj, name)
    if isinstance(prop, _RemoteMethodStub):
      def _remote_call(stub):
        def _inner(*args):
          # print host_descriptor, 'REMOTE CALL', stub, args
          return channel_manager._send_call(host_descriptor, interface_name, stub, args)
        return _inner
      setattr(interface_obj, name, _remote_call(prop))
  # print 'REMOTE OBJ', interface_obj
  return interface_obj


TAG_MESSAGE = 1
TAG_MESSAGE_ACK = 2
TAG_MESSAGE_RESULT = 3

class Channel(object):  # TODO: basechannel?
  def __init__(self, channel_manager, pipe):
    self.channel_manager = channel_manager
    self.pipe = pipe
    self.seq = 0
    self.lock = threading.Lock()
    self.ack_conds = {}
    self.result_conds = {}
    self.results = {}
    rt = threading.Thread(target=self.recv_thread)
    rt.daemon = True
    rt.start()

  def send_message(self, message_bytes, wait_for_ack=True, wait_for_result=True):
    assert isinstance(message_bytes, str)
    message_id = self.seq
    self.seq += 1
    # TODO: long longs?
    with self.lock:
      self.pipe.send(struct.pack('<iii', TAG_MESSAGE, message_id, len(message_bytes)))
      self.pipe.send(message_bytes)
      if wait_for_ack:
        self.ack_conds[message_id] = threading.Condition(self.lock)
        self.ack_conds[message_id].wait()
      if wait_for_result:
        # print 'WAIT FOR RESULT...'
        self.result_conds[message_id] = threading.Condition(self.lock)
        self.result_conds[message_id].wait()
        # print 'GOT RESULT...', self.results[message_id]
        return self.results[message_id]
    # return message_id

  def recv_thread(self):
    while True:
      # print 'RECV TAG'
      tag, = struct.unpack('<i', self.pipe.recv(4))
      if tag == TAG_MESSAGE:
        # print 'RECEIVED MESSAGE'
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        message_len, = struct.unpack('<i', self.pipe.recv(4))
        # print 'MESSAGE LEN', message_len
        message_bytes = self.pipe.recv(message_len)
        while len(message_bytes) < message_len:
          message_bytes += self.pipe.recv(message_len - len(message_bytes))
        with self.lock:
          self.pipe.send(struct.pack('<ii', TAG_MESSAGE_ACK, message_id))
        return_val = self._process_received_message(message_id, message_bytes)
        return_bytes = pickle.dumps(return_val)
        with self.lock:
          self.pipe.send(struct.pack('<iii', TAG_MESSAGE_RESULT, message_id, len(return_bytes)))
          self.pipe.send(return_bytes)
      elif tag == TAG_MESSAGE_ACK:
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        # print 'RECEIVED MESSAGE ACK', message_id
        with self.lock:
          cond = self.ack_conds.get(message_id)
          if cond:
            cond.notify()
      elif tag == TAG_MESSAGE_RESULT:
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        message_len, = struct.unpack('<i', self.pipe.recv(4))
        # print 'RECEIVED MESSAGE RESULT', message_id
        message_bytes = self.pipe.recv(message_len)
        with self.lock:
          self.results[message_id] = pickle.loads(message_bytes)
          cond = self.result_conds.get(message_id)
          if cond:
            cond.notify()
      else:
        # print repr((TAG_MESSAGE, TAG_MESSAGE_ACK)), tag, tag == TAG_MESSAGE
        raise Exception('UNKNOWN TAG %r' % tag)



  def _process_received_message(self, message_id, message_bytes):
    # print 'RECCEIVED', message_id, repr(message_bytes)
    interface_name, method_stub_name, args = pickle.loads(message_bytes)
    # print 'YO', method_stub_name, args
    return self.channel_manager.call_local(interface_name, method_stub_name, args)


class LoopbackPipe(object):
  def __init__(self):
    self.lock = threading.Lock()
    self.new_data_cond = threading.Condition(self.lock)
    self.buffer = ''

  def send(self, body):
    # print 'SEND', repr(body)
    with self.lock:
      self.buffer += body
      self.new_data_cond.notify()

  def recv(self, max_len):
    # print 'RECV WAIT', max_len
    with self.lock:
      while len(self.buffer) == 0:
        self.new_data_cond.wait()
      body = self.buffer[0:max_len]
      self.buffer = self.buffer[max_len:]
    # print 'RECV\'d', len(body), repr(body)
    return body



def loopback(s2):
  while True:
    a = s2.recv(2048)
    if a:
      s2.send(a)


class ChannelManager(object):
  def __init__(self, host_descriptor=None):
    # TODO: options
    self.host_descriptor = host_descriptor or HostDescriptor.generate()
    self.interfaces = {}
    self.channels = {}
    s1, s2 = socket.socketpair()
    lt = threading.Thread(target=loopback, args=(s2,))
    lt.daemon = True
    lt.start()
    # self.channels[self.host_descriptor.host_id] = Channel(self, s1)
    self._listen()

  def _listen(self):
    pass

  def _add_channel(self, host_descriptor, channel):
    self.channels[host_descriptor.host_id] = channel



  def register_interface(self, name, interface):
    self.interfaces[name] = interface

  def get_interface(self, host_descriptor, name, interface_cls):
    return get_remote_interface(host_descriptor, self, name, interface_cls)

  def _send_call(self, host_descriptor, interface_name, method_stub, args):
    # print self.host_descriptor, 'SEND CALL', host_descriptor, method_stub, args
    payload = pickle.dumps((interface_name, method_stub.name, args))
    start_time = time.time()
    result = self.channels[host_descriptor.host_id].send_message(payload, wait_for_ack=False)
    end_time = time.time()
    # print self.host_descriptor, 'SEND CALL SEND MESSAGE DURATION:', end_time - start_time
    return result

  def call_local(self, interface_name, method_stub_name, args):
    # print self.host_descriptor, 'CALL_LOCAL', method_stub_name, args
    interface = self.interfaces[interface_name]
    return getattr(interface, method_stub_name)(*args)


class ChannelMode:
  TCP = 'tcp'
  UNIX = 'unix'

class ChannelConfig(object):
  def __init__(self, host_id=None,
               listen=False, listen_mode=ChannelMode.TCP, listen_address='localhost', listen_port=-1,
               connect=False, connect_mode=ChannelMode.TCP, connect_address='localhost', connect_port=-1):
    self.host_id = host_id
    self.listen = listen
    self.listen_mode = listen_mode
    self.listen_address = listen_address
    self.listen_port = listen_port
    self.connect = connect
    self.connect_mode = connect_mode
    self.connect_address = connect_address
    self.connect_port = connect_port

  @staticmethod
  def from_dict(data):
    return ChannelConfig(**data)

  def to_dict(self):
    return {
      'host_id': self.host_id,
      'listen': self.listen,
      'listen_mode': self.listen_mode,
      'listen_address': self.listen_address,
      'listen_port': self.listen_port,
      'connect': self.connect,
      'connect_mode': self.connect_mode,
      'connect_address': self.connect_address,
      'connect_port': self.connect_port,
    }



def set_channel_config(config):
  assert isinstance(config, ChannelConfig)
  globals()['_channel_config'] = config

def get_channel_config():
  if '_channel_config' in globals():
    return globals()['_channel_config']
  return ChannelConfig()


def _initialize_pipe(pipe, my_host_id):
  pipe.send(struct.pack('<i', my_host_id))
  other_host_id, = struct.unpack('<i', pipe.recv(4))
  # print 'INITAILIZED: my_host_id', my_host_id, 'other', other_host_id
  return other_host_id


def get_channel_manager():
  if '_channel_manager' in globals():
    # TODO: there is probably some initialization race condition here if not called while single-threaded...
    return globals()['_channel_manager']
  # construct channel_manager
  config = get_channel_config()
  if config.host_id:
    host_descriptor = HostDescriptor(config.host_id)
  else:
    host_descriptor = HostDescriptor.generate()
  manager = ChannelManager(host_descriptor=host_descriptor)
  if config.listen:
    listener = ChannelListener(manager, config.listen_mode, config.listen_address, config.listen_port)
    listener.start()
  if config.connect:
    if config.connect_mode == ChannelMode.TCP:
      pipe = socket.create_connection((config.connect_address, config.connect_port))
    elif config.connect_mode == ChannelMode.UNIX:
      pipe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      pipe.connect(config.connect_address)
    else:
      raise ValueError('Invalid connect mode: %r.' % config.connect_mode)
    # perform initialization sequence:
    other_host_id = _initialize_pipe(pipe, host_descriptor.host_id)
    channel = Channel(manager, pipe)
    manager.channels[other_host_id] = channel
  globals()['_channel_manager'] = manager
  return manager


class Interface(object):
  pass


class _RemoteMethodStub(object):
  def __init__(self, name, arg_types, return_type=None):
    self.name = name
    self.arg_types = arg_types
    self.return_type = return_type

def remote_method(*arg_types, **kvargs):
  return_type = kvargs.get('returns')
  def _inner(method):
    return _RemoteMethodStub(method.__name__, arg_types, return_type)
  return _inner

class ChannelListener(threading.Thread):
  def __init__(self, channel_manager, listen_mode, listen_address, listen_port):
    self.channel_manager = channel_manager
    self.listen_mode = listen_mode
    self.listen_address = listen_address
    self.listen_port = listen_port
    super(ChannelListener, self).__init__()
    self.daemon = True

  def run(self):
    if self.listen_mode == ChannelMode.TCP:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.bind((self.listen_address, self.listen_port))
    elif self.listen_mode == ChannelMode.UNIX:
      s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      s.bind(self.listen_address)
    s.listen(1)  # Number of queued connections.
    while True:
      conn, addr = s.accept()
      # print 'accepted', conn, addr
      other_host_id = _initialize_pipe(conn, self.channel_manager.host_descriptor.host_id)
      # print 'other_host_id', other_host_id
      # print 'ChannelListener got new', conn, addr
      channel = Channel(self.channel_manager, conn)
      # print 'registered new channel for host id', other_host_id
      self.channel_manager.channels[other_host_id] = channel

## END RPC layer stuff