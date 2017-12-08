

from apache_beam.runners.laser.graph import CompositeWatermarkNode
from apache_beam.runners.laser.graph import PCollectionNode
from apache_beam.runners.laser.graph import WatermarkNode
from apache_beam.runners.laser.graph import ReadStep
from apache_beam.runners.laser.graph import ShuffleReadStep
from apache_beam.runners.laser.graph import ShuffleWriteStep
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.runners.worker import operation_specs



class ExecutionGraph(object):
  def __init__(self):
    self.stages = []

  def add_stage(self, stage):
    self.stages.append(stage)

class Stage(object):  # TODO: rename to ExecutionStage
  def __init__(self):
    super(Stage, self).__init__()



class ShuffleFinalizeStage(Stage, WatermarkNode):
  def __init__(self, name, execution_context, dataset_id):
    super(ShuffleFinalizeStage, self).__init__()
    self.name = name
    self.execution_context = execution_context
    self.dataset_id = dataset_id
    # Hold watermark on following steps until execution progress is made.
    self.set_watermark_hold(MIN_TIMESTAMP)

  def initialize(self):
    pass

  def start(self):
    # self.execution_context.shuffle_interface.commit_transaction(self.dataset_id, 0)
    self.execution_context.shuffle_interface.finalize_dataset(self.dataset_id)
    print 'FINALIZED!!'
    self.set_watermark_hold(None)
    # print self.execution_context.shuffle_interface.read(
    #   self.dataset_id,
    #   LexicographicRange(
    #     LexicographicPosition.KEYSPACE_BEGINNING,
    #     LexicographicPosition.KEYSPACE_END), None, 10000000)


  def input_watermark_advanced(self, new_watermark):
    print 'ShuffleFinalizeStage input watermark ADVANCED', new_watermark
    if new_watermark == MAX_TIMESTAMP:
      self.start()  # TODO: reconcile all the run / start method names.






class FusedStage(Stage, CompositeWatermarkNode):

  def __init__(self, name, execution_context):
    super(FusedStage, self).__init__()
    self.name = name
    self.execution_context = execution_context

    self.step_to_original_step = {}
    self.original_step_to_step = {}
    self.read_step = None

    # Topologically sorted steps.
    self.steps = []
    # Step to index in self.steps.
    self.step_index = {}

    self.shuffle_dataset_ids = []

    self.pending_work_item_ids = set()
    # Hold watermark on all steps until execution progress is made.
    self.input_watermark_node.set_watermark_hold(MIN_TIMESTAMP)

  def add_step(self, original_step, origin_step=None, side_input_map={}):
    # when a FusedStage adds a step, the step is copied.
    step = original_step.copy()
    # self.step_to_original_step[step] = original_step
    if not (origin_step or original_step) in self.original_step_to_step: # TODO: HACKHACK for fusion break materialization
      self.original_step_to_step[origin_step or original_step] = step  # TODO: HACK
    # Replicate outputs.
    for tag, original_output_pcoll in original_step.outputs.iteritems():
      step.outputs[tag] = PCollectionNode(step, tag)

    if isinstance(step, ReadStep):
      assert not original_step.inputs
      assert not self.read_step
      self.read_step = step
      self.input_watermark_node.add_dependent(step)
    elif isinstance(step, ShuffleReadStep):
      assert not original_step.inputs
      assert not self.read_step
      self.read_step = step
      self.input_watermark_node.add_dependent(step)
    else:
      if isinstance(step, ShuffleWriteStep):
        self.shuffle_dataset_ids.append(step.dataset_id)
      # Copy inputs.
      for original_input_pcoll in original_step.inputs:
        input_step = self.original_step_to_step[original_input_pcoll.step]
        # input_pcoll = input_step.outputs[tag]  # TODO: wait WTF is tag?
        print 'original_step', original_step
        print 'input_step', input_step
        input_pcoll = input_step.outputs[None]  # TODO: fix for multiple outputs
        step.inputs.append(input_pcoll)
        input_pcoll.add_consumer(step)
        input_step.add_dependent(step)
      # TODO: copy side inputs
    self.steps.append(step)
    self.step_index[step] = len(self.steps) - 1
    return step

  def finalize_steps(self):
    for step in self.steps:
      # TODO: we actually only need to add the ones that don't have children
      step.add_dependent(self.output_watermark_node)


  def __repr__(self):
    return 'FusedStage(name: %s, steps: %s)' % (self.name, self.steps)

  def input_watermark_advanced(self, new_watermark):
    print 'FUSEDSTAGE input watermark ADVANCED', new_watermark
    if new_watermark == MAX_TIMESTAMP:
      self.start()  # TODO: reconcile all the run / start method names.

  def start(self):
    print 'STARTING FUSED STAGE', self

    print 'MAPTASK GENERATION!!'
    all_operations = list(step.as_operation(self.step_index) for step in self.steps)

    # Perform initial source splitting.
    read_op = all_operations[0]
    split_read_ops = []
    READ_SPLIT_TARGET_SIZE = 16 * 1024 *  1024
    # READ_SPLIT_TARGET_SIZE = 1 * 1024 * 1024 # TODO: tune
    if isinstance(read_op, operation_specs.WorkerRead):
      # split_source_bundles = [read_op.source]
      split_source_bundles = list(read_op.source.source.split(READ_SPLIT_TARGET_SIZE))
      # print '!!! SPLIT OFF', split_source_bundles
      for source_bundle in split_source_bundles:
        new_read_op = operation_specs.WorkerRead(source_bundle, read_op.output_coders)
        split_read_ops.append(new_read_op)
    elif isinstance(read_op, operation_specs.LaserShuffleRead):
      # TODO: do size-based initial splitting of shuffle read operation, based on dataset characteristics,
      # i.e. try to binary search to find appropriate split points.
      key_range_summary = self.execution_context.shuffle_interface.summarize_key_range(read_op.dataset_id, read_op.key_range)
      dataset_size = key_range_summary.byte_count
      print 'DATASET SIZE', dataset_size, key_range_summary
      split_count = max(4, int(round(dataset_size / READ_SPLIT_TARGET_SIZE)))
      # split_count = 1
      split_ranges = read_op.key_range.split(split_count)
      for split_range in split_ranges:
        new_read_op = operation_specs.LaserShuffleRead(
          dataset_id=read_op.dataset_id,
          key_range=split_range,
          output_coders=read_op.output_coders,
          grouped=read_op.grouped,
          )
        split_read_ops.append(new_read_op)
    else:
      raise Exception('First operation should be a read operation: %s.' % read_op)


    for removeme_i, split_read_op in enumerate(split_read_ops):
      ops = [split_read_op] + all_operations[1:]
      step_names = list('s%s' % ix for ix in range(len(self.steps)))
      system_names = step_names
      original_names = list(step.name for step in self.steps)
      map_task = operation_specs.MapTask(ops, self.name, system_names, original_names, original_names)
      print 'MAPTASK', map_task
      # counter_factory = CounterFactory()
      # state_sampler = statesampler.StateSampler(self.name, counter_factory)
      # map_executor = operations.SimpleMapTaskExecutor(map_task, counter_factory, state_sampler)
      # map_executor.execute()
      print 'SCHEDULING WORK (%d / %d)...' % (removeme_i + 1, len(split_read_ops))
      work_item_id = self.execution_context.work_manager.schedule_map_task(self, map_task)
      self.pending_work_item_ids.add(work_item_id)
    print 'DONE SCHEDULING WORK!'
    # self.input_watermark_node.set_watermark_hold(None)

  def report_work_completion(self, work_item):
    work_item_id = work_item.id
    self.pending_work_item_ids.remove(work_item_id)

    # Commit shuffle transactions.
    for dataset_id in set(self.shuffle_dataset_ids):
      # TODO: consider doing a 2-phase prepare-commit.
      self.execution_context.shuffle_interface.commit_transaction(dataset_id, work_item.transaction_id)

    print 'COMPLETION', self, work_item_id, self.pending_work_item_ids
    if not self.pending_work_item_ids:
      # Stage completed!  Let's release the watermark.
      self.input_watermark_node.set_watermark_hold(None)


  def initialize(self):
    print 'INIT'
