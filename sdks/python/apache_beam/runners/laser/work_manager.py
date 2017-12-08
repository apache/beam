

from Queue import Queue
import threading
import time

from apache_beam.internal import pickler
from apache_beam.runners.laser.channels import get_channel_manager
from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import remote_method

class WorkItem(object):
  def __init__(self, id, stage_name, map_task, transaction_id):
    self.id = id  # TODO: rename to work_id or worK_item_id?
    self.stage_name = stage_name
    self.map_task = map_task
    self.transaction_id = transaction_id
    # TODO: attempt number


class WorkItemStatus(object):
  NOT_STARTED = 0
  RUNNING = 1
  COMPLETED = 2
  FAILED = 3


# TODO: some work item progress / execution info

class WorkManagerInterface(Interface):
  @remote_method(int, int)
  def report_work_status(self, work_item_id, new_status):
    raise NotImplementedError()

  @remote_method(str)
  def register_worker(self, worker_id):
    pass


class WorkManager(WorkManagerInterface, threading.Thread):  # TODO: do we need a separate worker pool manager?
  def __init__(self, node_manager):
    super(WorkManager, self).__init__()
    self.channel_manager = get_channel_manager()

    self.node_manager = node_manager
    self.work_items = {}
    self.unscheduled_work = Queue()  # TODO: should this just be a set?

    self.work_status = {}
    self.work_stage = {}
    self.next_work_transaction_id = 0
    self.lock = threading.Lock()
    self.event_condition = threading.Condition(self.lock)

    self.worker_interfaces = {}
    self.all_workers = set()
    self.idle_workers = set()
    self.active_workers = set()

    self.first_schedule_time = None

  def schedule_map_task(self, stage, map_task):  # TODO: should we track origin?  (yes)
    with self.lock:
      if not self.first_schedule_time:
        self.first_schedule_time = time.time()
      work_item_id = len(self.work_items)
      print 'SCHEDULE_MAP_TASK', stage, map_task, work_item_id
      transaction_id = self.next_work_transaction_id
      self.next_work_transaction_id += 1
      work_item = WorkItem(work_item_id, stage.name, map_task, transaction_id)
      self.work_items[work_item_id] = work_item
      self.work_status[work_item] = WorkItemStatus.NOT_STARTED
      self.work_stage[work_item] = stage
      self.unscheduled_work.put(work_item)
      self.event_condition.notify()
    return work_item_id

  def run(self):
    print 'WORK MANAGER STARTING'
    self.channel_manager.register_interface('master/work_manager', self)
    print 'STARTING COMPUTE NODES'
    for node_stub in self.node_manager.get_nodes():
      while True:
        try:
          print 'START COMPUTE', node_stub, node_stub.start_worker()
          break
        except InterfaceNotReadyException:
          print 'NOT READYU'
          pass
    while True:
      print 'WORK MANAGER POLL', self.unscheduled_work, self.idle_workers
      keep_scheduling = True
      to_execute = []
      while keep_scheduling:
        with self.lock:
          work_item = None
          worker_interface = None
          if not self.unscheduled_work.empty() and self.idle_workers:
            work_item = self.unscheduled_work.get()
            worker_id = self.idle_workers.pop()
            self.active_workers.add(worker_id)
            worker_interface = self.worker_interfaces[worker_id]
            to_execute.append((worker_interface, work_item))
            self.work_status[work_item] = WorkItemStatus.RUNNING  # TODO: do we want more granular status?
          else:
            keep_scheduling = False
      print 'SCHEDULING', to_execute
      if self.first_schedule_time:
        print '%f SECOND ELAPSED SINCE FIRST WORK SCHEDULED' % (time.time() - self.first_schedule_time)
      for worker_interface, work_item in to_execute:
        worker_interface.schedule_work_item(pickler.dumps(work_item))

      with self.lock:
        if not self.unscheduled_work.empty() and self.idle_workers:
          continue
        else:
          self.event_condition.wait()

  # REMOTE METHOD
  def register_worker(self, worker_id):
    print '********************REGISTER WORKER', worker_id
    from apache_beam.runners.laser.worker import WorkerInterface
    with self.lock:
      # TODO: get a better worker wrapper class.
      worker_interface = self.channel_manager.get_interface('%s/worker' % worker_id, WorkerInterface)
      self.worker_interfaces[worker_id] = worker_interface
      self.all_workers.add(worker_id)
      self.idle_workers.add(worker_id)
      self.event_condition.notify()

  # REMOTE METHOD
  def report_work_status(self, worker_id, work_item_id, new_status):
    # TODO: generation id / attempt number
    start_time = time.time()
    with self.lock:
      # TODO: some validation
      work_item = self.work_items[work_item_id]
      assert self.work_status[work_item] == WorkItemStatus.RUNNING
      if new_status == WorkItemStatus.COMPLETED:
        self.work_status[work_item] = WorkItemStatus.COMPLETED
      elif new_status == WorkItemStatus.FAILED:
        self.work_status[work_item] = WorkItemStatus.FAILED
      else:
        raise ValueError('Invalid WorkItemStatus: %d' % new_status)
    if new_status == WorkItemStatus.COMPLETED:
      self.work_stage[work_item].report_work_completion(work_item)
    elif new_status == WorkItemStatus.FAILED:
      # TODO: retry, failure count
      with self.lock:
        self.work_status[work_item] = WorkItemStatus.NOT_STARTED
        transaction_id = self.next_work_transaction_id
        self.next_work_transaction_id += 1
        work_item.transaction_id = transaction_id
        self.unscheduled_work.put(work_item)
    with self.lock:
      self.active_workers.remove(worker_id)
      self.idle_workers.add(worker_id)
      self.event_condition.notify()
    # worker = self.active_workers[worker_id]
    # del self.active_workers[worker_id]
    elapsed_time = time.time() - start_time
    print 'report_work_status %s, %s took %fs' % (worker_id, work_item_id, elapsed_time)
