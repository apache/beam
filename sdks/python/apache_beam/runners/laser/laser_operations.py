
import logging

from apache_beam.coders import coders
from apache_beam.runners.worker.operations import Operation
from apache_beam.transforms.window import GlobalWindows
from apache_beam.runners.laser.channels import get_channel_manager


WRITE_BUFFER_SIZE = 8 * 1024 * 1024

class ShuffleWriteOperation(Operation):
  """A shuffle write operation."""

  def __init__(self, operation_name, spec, work_context, counter_factory, state_sampler):
    super(ShuffleWriteOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    self.dataset_id = spec.dataset_id
    self.transaction_id = work_context.transaction_id
    self.element_coder = spec.output_coders[0]
    self.key_coder = self.element_coder.key_coder()
    self.value_coder = self.element_coder.value_coder()

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
      import random
      # if random.randint(0,2) == 0:  # for testing retrying transactions
      #   raise Exception('wtf')
      # print 'Ele', element
      # print 'coder', self.element_coder
      # print 'Vcoder', self.value_coder
      key, value = element.value
      encoded_key = self.key_coder.encode(key)
      encoded_value = self.value_coder.encode(value)
      # print 'E', repr(encoded_key), repr(encoded_value)
      self.write_buffer.append((encoded_key, encoded_value))
      self.buffer_bytes += len(encoded_key) + len(encoded_value)
      if self.buffer_bytes >= WRITE_BUFFER_SIZE:
        self._flush()

  def _flush(self):
    if self.write_buffer:
      print 'FLUSH (current buffer has %d bytes)' % self.buffer_bytes
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
    self.element_coder = spec.output_coders[0]
    self.key_coder = self.element_coder.wrapped_value_coder.key_coder()
    self.value_coder = coders.WindowedValueCoder(self.element_coder.wrapped_value_coder.value_coder().value_coder())

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
          encoded_key, encoded_value = element
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
        if continuation_token is None:
          break
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



