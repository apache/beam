
import cProfile
import pstats
import threading
import time

from apache_beam.internal import pickler
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.runners.laser.channels import get_channel_manager
from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import remote_method
from apache_beam.runners.laser.work_manager import WorkItemStatus
from apache_beam.runners.laser.work_manager import WorkManagerInterface
from apache_beam.utils.counters import CounterFactory
try:
  from apache_beam.runners.worker import statesampler
except ImportError:
  from apache_beam.runners.worker import statesampler_fake as statesampler
from apache_beam.runners.worker import operations

class WorkerInterface(Interface):

  @remote_method(returns=str)
  def ping(self):
    raise NotImplementedError()

  @remote_method(object)
  def schedule_work_item(self, work_item):
    raise NotImplementedError()


class WorkContext(object):
  def __init__(self, transaction_id):
    self.transaction_id = transaction_id


class LaserWorker(WorkerInterface, threading.Thread):
  def __init__(self, config):
    super(LaserWorker, self).__init__()
    self.config = config
    self.worker_id = config['name']

    self.channel_manager = get_channel_manager()
    self.work_manager = self.channel_manager.get_interface('master/work_manager', WorkManagerInterface)

    self.lock = threading.Lock()
    self.current_work_item = None
    self.new_work_condition = threading.Condition(self.lock)

  def ping(self):
    return 'OK'

  def run(self):
    self.channel_manager.register_interface('%s/worker' % self.worker_id, self)
    time.sleep(2)  # HACK
    self.work_manager.register_worker(self.worker_id)
    while True:
      with self.lock:
        while not self.current_work_item:
          self.new_work_condition.wait()
        work_item = self.current_work_item
      counter_factory = CounterFactory()
      state_sampler = statesampler.StateSampler(work_item.stage_name, counter_factory)
      work_context = WorkContext(work_item.transaction_id)
      MetricsEnvironment.set_metrics_supported(False)
      map_executor = operations.SimpleMapTaskExecutor(work_item.map_task, counter_factory, state_sampler, work_context=work_context)
      status = WorkItemStatus.COMPLETED
      try:
        print '>>>>>>>>> WORKER', self.worker_id, 'EXECUTING WORK ITEM', work_item.id, work_item.map_task
        print 'operations:', work_item.map_task.operations
        start_time = time.time()
        pr = cProfile.Profile()
        pr.enable()
        map_executor.execute()
        pr.disable()
        end_time = time.time()
        sortby = 'cumtime'
        # sortby = 'tottime'
        ps = pstats.Stats(pr).sort_stats(sortby)
        time_taken = end_time - start_time
        print '<<<<<<<<< WORKER DONE (%fs)' % time_taken, self.worker_id, 'EXECUTING WORK ITEM', work_item.id, work_item.map_task
        # ps.print_stats()
      except Exception as e:
        print 'Exception while processing work:', e
        import traceback
        traceback.print_exc()
        status = WorkItemStatus.FAILED
      with self.lock:
        self.current_work_item = None
      self.work_manager.report_work_status(self.worker_id, work_item.id, status)



  # REMOTE METHOD
  def schedule_work_item(self, serialized_work_item):
    work_item = pickler.loads(serialized_work_item)
    with self.lock:
      print 'SCHEDULED', work_item.id
      if self.current_work_item:
        raise Exception('Currently executing work item: %s' % self.current_work_item)
      self.current_work_item = work_item
      self.new_work_condition.notify()
