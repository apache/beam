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

# cython: profile=True

"""Element execution tracker for Dataflow service.

Each processed element through each step in the fused stage gets tracked as a
ScopedState. Processing time is sampled using StateSampler and distributed
among all elements which are executed since the last sample period.

Elements are processed in potentially many fragments of execution as we move up
and down the stage graph via outputting. Each fragment of execution is counted
equally for attributing sampled processing time.

When an element is finished processing it is held in the doneJournal collection
until the next sampling round in order to calculate the final processing time.
Eventually the total element processing time is reported to the counter and the
state is cleaned up.

Implementation itself is not thread-safe.

Concurrency situation: For multiple execution threads and execution threads with
sampling thread, we use StateSampler.lock to ensure safety. For report_progress
thread, we only report counters(access to counter_cache) when a work_item
completed, which means, there is no other executions and sampling at the same
time.

For internal use only; no backwards-compatibility guarantees.

"""

cimport cython
from libc.stdlib cimport malloc, free

from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterName
from apache_beam.transforms.cy_dataflow_distribution_counter cimport DataflowDistributionCounter

from cython.operator cimport dereference as dref
from cython.operator cimport preincrement as iref

cdef class Journal(object):
  """A collection of elements

  Deque-based data structure for passing journaled events between a single
  journal reader and single journal writer.

  Each event is journaled with an externally-managed snapshot version. Snapshot
  versions are unique and monotonically increasing.

  No thread-safe guarantees. Needs lock during operation.

  Attributes:
    max_snapshot: keep track of latest snapshot
    journal: a deque keeping SnapshottedExecutionPtrs
  """
  def __init__(self):
    self.max_snapshot = -1
    self.journal = deque[SnapshottedExecutionPtr]()

  cdef void add_journal(self, ElementExecutionPtr execution_ptr,
                        int64_t snapshot):
    """Create SnapshottedExecutionPtr and append to journal.
    
    Args:
      execution_ptr: Pointer of ElementExecution for new journal element.
                     Could be NULL if current element is IDLE.
      snapshot: New snapshot for journal element.
    """
    if snapshot <= self.max_snapshot:
      raise ValueError('Timestamps must be monotonically increasing.'
                       + 'Input snapshot %d is not greater than max snapshot %d'
                       % (snapshot, self.max_snapshot))
    cdef SnapshottedExecutionPtr snapshotted_ptr = (
      <SnapshottedExecutionPtr> malloc(sizeof(SnapshottedExecution)))
    assert(snapshotted_ptr != NULL)
    snapshotted_ptr.execution_ptr = execution_ptr
    snapshotted_ptr.snapshot = snapshot
    self.journal.push_back(snapshotted_ptr)
    self.max_snapshot = snapshot

cdef class ReaderWriterState(object):
  """State shared between ExecutionJournalReader and ExecutionJournalWriter.

  Attributes:
    execution_journal: Journal of fragments of execution per element to count
    for attributing processing time. Each time we transition up or down
    the stage fusion graph we add an execution fragment for the currently
    processing element with an incremented snapshot version. Each snapshot
    version must have a representative value, or NULL to represent completion
    of processing.
    done_journal: Elements which have completed processing but are still pending
    final timing calculation. The elements are moved here when they exit the
    ExecutionJournalWriter.execution_stack and are held here until the counter
    value can be reported in the next sampling round.
    latest_snapshot: Monotonically increasing snapshot version number which
    tracks the latest execution state ready to be reported. Snapshot versions
    are associated with values in the execution_journal and done_journal.
    This value written by ExecutionJournalWriter, and read by the
    ExecutionJournalReader.
  """
  def __init__(self):
    self.latest_snapshot = 0
    self.execution_journal = Journal()
    self.done_journal = Journal()

cdef class ExecutionJournalReader(object):
  """Accounts sampled time to processed elements based on execution journal
  entries.

  Attributes:
    shared_state: Shared journals with ExecutionJournalWriter.
    execution_duration: Accumulated execution time per element. Once an element
    has finished processing and execution time has been attributed, the total
    execution time is reported via the counter and removed from the collection.

  Args:
    shared_state: Initialized in DataflowElementExecutionTracker.
  """
  def __init__(self, ReaderWriterState shared_state):
    self.shared_state = shared_state
    self.execution_duration = unordered_map[ElementExecutionPtr, int64_t]()

  cdef void take_sample(self, int64_t sample_time,
      unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil:
    """Sampling execution elements
    
    Account the specified processing time duration to elements which have 
    processed since the last sampling round, and update counter_cache for 
    completed elements.
    
    Args:
      sample_time: Total sampling time since last time.
      counter_cache: Initialized in DataflowElementExecutionTracker.
    """
    latest_snapshot = self.shared_state.latest_snapshot
    self.attribute_processing_time(sample_time, latest_snapshot)
    self.update_counter_cache(latest_snapshot, counter_cache)

  cdef void attribute_processing_time(self, int64_t sample_time,
                                      int64_t latest_snapshot) nogil:
    """Attribute processing time to elements from execution_journal up to the 
    specified snapshot. 
    """
    cdef int64_t total_execution = 0
    cdef unordered_map[ElementExecutionPtr, int64_t] executions_per_element
    cdef SnapshottedExecutionPtr snapshotted_ptr = NULL
    # Calculate total execution counts and prune execution_journal.
    while not self.shared_state.execution_journal.journal.empty():
      if (self.shared_state.execution_journal.journal.front().snapshot
          <= latest_snapshot):
        # Clean up SnapshottedExecutionPtr before reassigning new value.
        if snapshotted_ptr != NULL:
          free(snapshotted_ptr)
        total_execution += 1
        snapshotted_ptr = self.shared_state.execution_journal.journal.front()
        self.shared_state.execution_journal.journal.pop_front()
        if snapshotted_ptr.execution_ptr == NULL:
          continue
        executions_per_element[snapshotted_ptr.execution_ptr] += 1
      else:
        break
    # Keep the currently executing element in the journal.
    # So its remaining execution time is counted in the next sampling round.
    if snapshotted_ptr != NULL and snapshotted_ptr.snapshot == latest_snapshot:
      self.shared_state.execution_journal.journal.push_front(snapshotted_ptr)
    cdef unordered_map[ElementExecutionPtr, int64_t].iterator it = (
      executions_per_element.begin())
    cdef ElementExecutionPtr element_ptr = NULL
    cdef int64_t attribution_time = 0
    # Attribute processing time.
    while it != executions_per_element.end():
      element_ptr = dref(it).first # get key of current map_item
      attribution_time = (sample_time / total_execution
                         * executions_per_element[element_ptr])
      self.execution_duration[element_ptr] += attribution_time
      iref(it) # next

  cdef void update_counter_cache(self, int64_t latest_snapshot,
      unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil:
    """Update counter_cache with values of done elements up to the given 
    snapshot and prune done_journals.
    """
    cdef SnapshottedExecutionPtr snapshotted_ptr = NULL
    cdef ElementExecutionPtr element_ptr = NULL
    while not self.shared_state.done_journal.journal.empty():
      snapshotted_ptr = self.shared_state.done_journal.journal.front()
      if snapshotted_ptr.snapshot <= latest_snapshot:
        self.shared_state.done_journal.journal.pop_front()
        element_ptr = snapshotted_ptr.execution_ptr
        # Unit of sampling time is nano secs, we update counter_cache with ms.
        (counter_cache[element_ptr.operation_name]
         .push_back(self.execution_duration[element_ptr]/1000000))
        # Clean up SnapshottedExecutionPtr & ElementExecutionPtr after updating
        # counter
        free(snapshotted_ptr)
        free(element_ptr)
      else:
        break


cdef class ExecutionJournalWriter(object):
  """Writes journal entries on element processing state changes.

  Attributes:
    shared_state: Shared journals with ExecutionJournalReader.
    execution_stack: Execution stack of processing elements. Elements are pushed
    onto the stack when scoped_process_state.__enter__ and popped on
    scoped_process_state.__exit__. This stack mirrors the actual runtime stack
    and contains the step + element context in order to attribute sampled
    execution time.

  Args:
    shared_state: Initialized in DataflowElementExecutionTracker.
  """
  def __init__(self, ReaderWriterState shared_state):
    self.shared_state = shared_state
    self.execution_stack = deque[ElementExecutionPtr]()
    self.add_execution(NULL)

  cdef void add_execution(self, ElementExecutionPtr execution_ptr):
    """Push current ElementExecution onto top of the execution_stack and add
    new entry into journal.
    """
    cdef int64_t next_snapshot = self.shared_state.latest_snapshot + 1
    self.execution_stack.push_back(execution_ptr)
    self.shared_state.execution_journal.add_journal(execution_ptr,
                                                    next_snapshot)
    self.shared_state.latest_snapshot = next_snapshot

  cdef void start_processing(self, char* operation_name):
    """ Called when processing_scoped_state.__enter__
    
    Create and journal a new ElementExecution to track a processing element.
    """
    cdef ElementExecutionPtr execution_ptr = (
      <ElementExecutionPtr> malloc(sizeof(ElementExecution)))
    assert (execution_ptr != NULL)
    execution_ptr.operation_name = operation_name
    self.add_execution(execution_ptr)

  cdef void done_processing(self):
    """ Called when processing_scoped_state.__exit__
    
    Indicates that the execution thread has exited the process method for 
    an element.When an element is finished processing, it is popped from the 
    execution stack, but will be tracked in the done_journal collection 
    until the next sampling round in order to account timing for the final 
    fragment of execution.
    """
    if self.execution_stack.size() <= 1:
      raise ValueError('No processing elements currently tracked.')
    cdef ElementExecutionPtr last_execution = self.execution_stack.back()
    cdef ElementExecutionPtr next_execution = NULL
    self.execution_stack.pop_back()
    cdef int64_t next_snapshot = self.shared_state.latest_snapshot + 1
    self.shared_state.done_journal.add_journal(last_execution, next_snapshot)
    next_execution = self.execution_stack.back()
    self.shared_state.execution_journal.add_journal(next_execution,
                                                    next_snapshot)
    self.shared_state.latest_snapshot = next_snapshot


cdef class DataflowElementExecutionTracker(object):
  """Implementation of DataflowElementExecutionTracker.

  Attributes:
    counter_cache: An map to cache processing time per element per step.
    cache_start_index: An map used for finding the range of time list to update.
    shared_state: ReaderWriterState shared between execution_reader
    and execution_writer.
    execution_reader: ExecutionJournalReader.
    execution_writer: ExecutionJournalWriter.
  """
  def __init__(self):
    self.counter_cache = unordered_map[CharPtr, vector[int64_t]]()
    self.cache_start_index = unordered_map[CharPtr, int64_t]()
    self.shared_state = ReaderWriterState()
    self.execution_reader = ExecutionJournalReader(self.shared_state)
    self.execution_writer = ExecutionJournalWriter(self.shared_state)

  cdef void enter(self, char* operation_name):
    self.execution_writer.start_processing(operation_name)

  cdef void exit(self):
    self.execution_writer.done_processing()

  cdef void take_sample(self, int64_t nanos_sampling_duration) nogil:
    self.execution_reader.take_sample(nanos_sampling_duration,
                                      self.counter_cache)

  cpdef void enter_for_test(self, char* operation_name):
    """Only visible for unit test.
    
    Need to keep enter func as cdef to making calling fast enough.
    """
    self.enter(operation_name)

  cpdef void exit_for_test(self):
    """Only visible for unit test.
    
    Need to keep exit func as cdef to make calling fast enough.
    """
    self.exit()

  cpdef void take_sample_for_test(self, int64_t nanos_sampling_duration):
    """Only visible for unit test.
    
    Need to keep take_sample as cdef to get rid of gil.
    """
    self.execution_reader.take_sample(nanos_sampling_duration,
                                      self.counter_cache)

  cpdef report_counter(self, counter_factory):
    """Only report counter when a work_item complete"""
    cdef unordered_map[CharPtr, vector[int64_t]].iterator map_it = (
      self.counter_cache.begin())
    cdef int64_t start_index = 0, end_index = 0
    cdef char* op_name
    cdef vector[int64_t] values
    cdef DataflowDistributionCounter time_counter = None
    while map_it != self.counter_cache.end():
      op_name = dref(map_it).first # get key of map_item
      values = dref(map_it).second # get value of map_item
      end_index = values.size()
      start_index = self.cache_start_index[op_name]
      counter_name = CounterName('per-element-processing-time',
                                 step_name=op_name)
      time_counter = counter_factory.get_counter(
          counter_name, Counter.DATAFLOW_DISTRIBUTION).accumulator
      while start_index < end_index:
        time_counter.add_input(values[start_index])
        start_index += 1
      self.cache_start_index[op_name] = end_index
      iref(map_it) # next
