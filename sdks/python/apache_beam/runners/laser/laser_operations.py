
import logging

from apache_beam.coders import coders
from apache_beam.runners.worker.operations import Operation
from apache_beam.transforms.window import GlobalWindows
from apache_beam.runners.laser.channels import get_channel_manager


WRITE_BUFFER_SIZE = 8 * 1024 * 1024


# key prefix hashing
import hashlib

HASH_LENGTH = 8

def prepend_key_hash(key):
  h = hashlib.md5()
  h.update(key)
  hash_bytes = h.digest()[0:HASH_LENGTH]
  return hash_bytes + key

def strip_hash(hash_and_key):
  return hash_and_key[HASH_LENGTH:]


class ShuffleWriteOperation(Operation):
  """A shuffle write operation."""

  def __init__(self, operation_name, spec, work_context, counter_factory, state_sampler):
    super(ShuffleWriteOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    self.dataset_id = spec.dataset_id
    self.transaction_id = work_context.transaction_id
    element_coder = spec.output_coders[0]
    self.element_coder = element_coder
    self.key_coder = element_coder.key_coder()
    self.value_coder = element_coder.value_coder()
    # self.value_coder = coders.VarIntCoder()

    channel_manager = get_channel_manager()
    from apache_beam.runners.laser.laser_runner import ShuffleWorkerInterface
    self.shuffle_interface = channel_manager.get_interface('master/shuffle', ShuffleWorkerInterface)

  def start(self):
    with self.scoped_start_state:
      self.write_buffer = []
      self.buffer_bytes = 0
      return
      super(ShuffleWriteOperation, self).start()
      # self.writer = self.spec.sink.writer()
      # self.writer.__enter__()
      # self.use_windowed_value = self.writer.takes_windowed_values

  def finish(self):
    with self.scoped_finish_state:
      self._flush()
      # self.writer.__exit__(None, None, None)

  def process(self, element):
    with self.scoped_process_state:
      # if random.randint(0,2) == 0:  # for testing retrying transactions
      #   raise Exception('wtf')
      # print 'Ecoder', self.element_coder
      # print 'Kcoder', self.key_coder
      # print 'Vcoder', self.value_coder
      key, value = element.value
      # print 'K, V', key, value
      encoded_key = self.key_coder.encode(key)
      # print 'ENCODED_KEY', repr(encoded_key)
      encoded_value = self.value_coder.encode(value)
      # print 'ENCODED_VALUE', repr(encoded_value)
      # print 'E', repr(encoded_key), repr(encoded_value)
      # hashed_encoded_key = prepend_key_hash(encoded_key)
      self.write_buffer.append((encoded_key, encoded_value))
      self.buffer_bytes += len(encoded_key) + len(encoded_value)
      if self.buffer_bytes >= WRITE_BUFFER_SIZE:
        self._flush()

  def _flush(self):
    if self.write_buffer:
      print 'FLUSH (current buffer has %d bytes)' % self.buffer_bytes
      print 'HELLO'
      print 'KEY CODER', self.key_coder
      print 'VALUE CODER', self.value_coder
      # print 'ELEMENT CODER', self.element_coder
      self.shuffle_interface.write(self.dataset_id, self.transaction_id, self.write_buffer)
      self.write_buffer = []
      self.buffer_bytes = 0


# Used to efficiently window the values of non-windowed side inputs.
_globally_windowed_value = GlobalWindows.windowed_value(None).with_value


class ShuffleReadOperation(Operation):
  def __init__(self, operation_name, spec, counter_factory, state_sampler):
    super(ShuffleReadOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    self.dataset_id = spec.dataset_id
    self.key_range = spec.key_range
    element_coder = spec.output_coders[0]
    self.element_coder = element_coder
    self.key_coder = element_coder.key_coder()
    self.value_coder = coders.WindowedValueCoder(element_coder.value_coder().value_coder(), coders.GlobalWindowCoder())
    # self.value_coder = coders.VarIntCoder()

    channel_manager = get_channel_manager()
    from apache_beam.runners.laser.laser_runner import ShuffleWorkerInterface
    self.shuffle_interface = channel_manager.get_interface('master/shuffle', ShuffleWorkerInterface)

  def start(self):
    # print '^^^^^^^^^^^^^^^^^^^^STARTED'
    with self.scoped_start_state:
      super(ShuffleReadOperation, self).start()
      has_active_key = False
      current_encoded_key = None
      current_values = None
      continuation_token = None
      while True:
        elements, continuation_token = self.shuffle_interface.read(
            self.dataset_id, self.key_range, continuation_token, 8 * 1024 * 1024)  # TODO: refactor into constant
        # print '^^^^^^^^^^^^^^^^^^^^READ', elements, continuation_token
        for element in elements:
          # print 'ELE', element
          # print 'KEY_CODER', self.key_coder
          # print 'VALUE_CODER', self.value_coder
          encoded_key, encoded_value = element
          # encoded_key = strip_hash(hashed_encoded_key)
          if has_active_key and current_encoded_key != encoded_key:
            current_key = self.key_coder.decode(current_encoded_key)
            # print '^^^^^^^^^^^^^^^^^^^^OUT', (current_key, current_values)
            self.output(_globally_windowed_value((current_key, current_values)))
            has_active_key = False
          if not has_active_key:
            current_encoded_key = encoded_key
            current_values = []
            has_active_key = True
          # print 'START TRY DECODE', self.value_coder, repr(encoded_value)
          current_values.append(self.value_coder.decode(encoded_value))
        print "<<< CONTINUATION TOKEN", continuation_token
        if continuation_token is None:
          break
      if has_active_key:
        current_key = self.key_coder.decode(current_encoded_key)
        # print '^^^^^^^^^^^^^^^^^^^^OUT', (current_key, current_values)
        self.output(_globally_windowed_value((current_key, current_values)))
      # print '^^^^^^^^^^^^^^^^^DONE'

  def finish(self):
    with self.scoped_finish_state:
      return
      # self.writer.__exit__(None, None, None)

  def process(self, element):
    with self.scoped_process_state:
      pass



