

import itertools

from apache_beam.runners.laser.graph import ReadStep
from apache_beam.runners.laser.graph import PCollectionNode
from apache_beam.runners.laser.graph import ParDoStep
from apache_beam.runners.laser.graph import GroupByKeyOnlyStep
from apache_beam.runners.laser.graph import FlattenStep
from apache_beam.runners.laser.graph import ShuffleReadStep
from apache_beam.runners.laser.graph import ShuffleWriteStep
from apache_beam.runners.laser.laser_operations import LaserSideInputSource
from apache_beam.runners.laser.lexicographic import LexicographicPosition
from apache_beam.runners.laser.lexicographic import LexicographicRange
from apache_beam.runners.laser.stages import ExecutionGraph
from apache_beam.runners.laser.stages import FusedStage
from apache_beam.runners.laser.stages import ShuffleFinalizeStage
from apache_beam.runners.worker import operation_specs

class ExecutionGraphTranslator(object):

  def __init__(self, step_graph, execution_context):
    self.step_graph = step_graph
    self.execution_context = execution_context

  def generate_execution_graph(self):
    # TODO: if we ever support interactive pipelines or incremental execution,
    # we want to implement idempotent application of a step graph into updating
    # the execution graph.
    self.execution_graph = ExecutionGraph()

    depths = {}
    def depth(step):
      if step in depths:
        return depths[step]
      if not step.inputs and not step.side_inputs:
        value = 0
      else:
        value = 1 + max(depth(s.step) for s in itertools.chain(step.inputs, step.side_inputs))
      depths[step] = value
      return value

    sorted_steps = sorted(self.step_graph.steps, key=lambda step: depth(step))

    self.next_shuffle_dataset_id = 0
    self.next_stage_id = 0

    self.pcoll_materialized_dataset_id = {}
    self.pcoll_materialization_finalization_steps = {}

    # Grow fused stages through a breadth-first traversal.
    self.steps_to_fused_stages = {}  # TODO: comment that this is output, for transformed steps
    for original_step in sorted_steps:
      # Process current step.
      # original_step = to_process.get()

      if isinstance(original_step, ReadStep):
        self._process_read_step(original_step)
      elif isinstance(original_step, ParDoStep):
        self._process_pardo_step(original_step)
      elif isinstance(original_step, GroupByKeyOnlyStep):
        self._process_gbk_step(original_step)
      elif isinstance(original_step, FlattenStep):
        self._process_flatten_step(original_step)
      else:
        raise ValueError('Execution graph translation not implemented: %s.' % original_step)

      for unused_tag, output_pcoll in original_step.outputs.iteritems():
        if output_pcoll.side_input_consumers:
          print 'CONSUMERRR', output_pcoll.side_input_consumers
          print 'SIDEINPUTTT', output_pcoll
          # Materialize this pcollection.
          shuffle_dataset_id = self.next_shuffle_dataset_id
          self.next_shuffle_dataset_id += 1
          self.execution_context.shuffle_interface.create_dataset(shuffle_dataset_id)
          write_stage = self._add_materialization_write(original_step, original_step.name + '/SIWrite', output_pcoll, original_step.element_coder, shuffle_dataset_id)
          finalize_stage = self._add_shuffle_finalize(shuffle_dataset_id, [write_stage])
          self.pcoll_materialized_dataset_id[output_pcoll] = shuffle_dataset_id
          self.pcoll_materialization_finalization_steps[output_pcoll] = finalize_stage

      # # Continue traversal of step graph.
      # for unused_tag, pcoll_node in original_step.outputs.iteritems():
      #   for consumer_step in itertools.chain(pcoll_node.consumers, pcoll_node.side_input_consumers):
      #     if consumer_step not in seen:
      #       to_process.put(consumer_step)
      #       seen.add(consumer_step)

    # Add fused stages to graph.
    for fused_stage in set(self.steps_to_fused_stages.values()):
      fused_stage.finalize_steps()

    return self.execution_graph

  def _process_read_step(self, original_step):
    assert not original_step.inputs
    stage_name = 'S%02d' % self.next_stage_id
    self.next_stage_id += 1
    fused_stage = FusedStage(stage_name, self.execution_context)
    self.execution_graph.add_stage(fused_stage)
    fused_stage.add_step(original_step)
    print 'fused_stage', original_step, fused_stage
    self.steps_to_fused_stages[original_step] = fused_stage

  def _process_pardo_step(self, original_step):
    print 'NAME', original_step.name
    # if original_step.name in ('GroupByKey/GroupByWindow'):
    #   print 'SKIPPING', original_step
    #   continue
    assert len(original_step.inputs) == 1
    input_pcoll = original_step.inputs[0]
    input_step = input_pcoll.step

    # If a side input depends on a step in the current FusedStage, we need to
    # do a fusion break and materialize before continuing.
    input_fused_stage = self.steps_to_fused_stages[input_step]
    do_fusion_break = False
    for side_input_node in original_step.side_inputs:
      side_input_step = side_input_node.step
      for step in input_fused_stage.original_step_to_step:
        if side_input_step.has_predecessor_step(step):
          do_fusion_break = True
          break

    if do_fusion_break:
      shuffle_dataset_id = self.next_shuffle_dataset_id
      self.next_shuffle_dataset_id += 1
      self.execution_context.shuffle_interface.create_dataset(shuffle_dataset_id)
      # input_fused_stage.shuffle_dataset_ids.append(shuffle_dataset_id)
      # TODO: should we have the element_coder on the pcoll instead for clarity
      print 'INPUT STEP', input_step
      print 'ORIGINAL STEP', original_step
      write_stage = self._add_materialization_write(input_step, input_step.name + '/FBWrite', input_pcoll, input_step.element_coder, shuffle_dataset_id)
      finalize_stage = self._add_shuffle_finalize(shuffle_dataset_id, [write_stage])
      fused_stage = self._add_materialization_read(input_step, input_step.name + '/FBRead', input_step.element_coder, shuffle_dataset_id, finalize_stage)
    else:
      fused_stage = input_fused_stage

    print 'input_step', input_step, type(input_step), original_step
    new_step = fused_stage.add_step(original_step)
    self.steps_to_fused_stages[original_step] = fused_stage

    # Inject side inputs into new step.
    side_input_sources = []
    for i, side_input_pcoll in enumerate(original_step.side_inputs):
      side_input_sources.append(operation_specs.WorkerSideInputSource(LaserSideInputSource(
        self.pcoll_materialized_dataset_id[side_input_pcoll],
        side_input_pcoll.step.element_coder,
        LexicographicRange(LexicographicPosition.KEYSPACE_BEGINNING, LexicographicPosition.KEYSPACE_END)
        ), str(i)))
      self.pcoll_materialization_finalization_steps[side_input_pcoll].add_dependent(fused_stage)
      # TODO: do we copy these over in add_step?
    new_step.side_input_sources = side_input_sources

    # TODO: add original step -> new step mapping.
    # TODO: add dependencies via WatermarkNode.

  def _process_gbk_step(self, original_step):
    # hallucinate a shuffle write and a shuffle read.
    # TODO: figure out lineage of hallucinated steps.
    assert len(original_step.inputs) == 1
    input_step = original_step.inputs[0].step
    fused_stage = self.steps_to_fused_stages[input_step]
    shuffle_dataset_id = self.next_shuffle_dataset_id
    self.next_shuffle_dataset_id += 1
    self.execution_context.shuffle_interface.create_dataset(shuffle_dataset_id)
    print 'SHUFFLE CODER', original_step.shuffle_coder
    shuffle_write_step = ShuffleWriteStep(original_step.name + '/GBKWrite', original_step.shuffle_coder, shuffle_dataset_id, grouped=True)
    print 'STEP', shuffle_write_step
    shuffle_write_step.inputs.append(original_step.inputs[0])
    print 'BEHOLD my hallucinated shuffle step', shuffle_write_step, 'original', original_step
    fused_stage.add_step(shuffle_write_step, origin_step=original_step)# HACK

    # Add shuffle finalization stage.
    stage_name = 'S%02d' % self.next_stage_id
    self.next_stage_id += 1
    finalize_stage = ShuffleFinalizeStage(stage_name, self.execution_context, shuffle_dataset_id)
    self.execution_graph.add_stage(finalize_stage)
    fused_stage.add_dependent(finalize_stage)

    # Add shuffle read stage, dependent on shuffle finalization.
    stage_name = 'S%02d' % self.next_stage_id
    self.next_stage_id += 1
    read_fused_stage = FusedStage(stage_name, self.execution_context)
    self.execution_graph.add_stage(read_fused_stage)
    read_step = ShuffleReadStep(original_step.name + '/GBKRead', original_step.output_coder, shuffle_dataset_id,
        LexicographicRange(LexicographicPosition.KEYSPACE_BEGINNING, LexicographicPosition.KEYSPACE_END), grouped=True)
    read_step.outputs[None] = PCollectionNode(read_step, None)
    read_fused_stage.add_step(read_step, origin_step=original_step)
    finalize_stage.add_dependent(read_fused_stage.input_watermark_node)

    # Output stage is the new read stage.
    self.steps_to_fused_stages[original_step] = read_fused_stage

  def _add_materialization_write(self, original_step, write_step_name, input_pcoll, element_coder, shuffle_dataset_id):
    input_step = input_pcoll.step
    fused_stage = self.steps_to_fused_stages[input_step]
    shuffle_write_step = ShuffleWriteStep(
        write_step_name,
        element_coder,
        shuffle_dataset_id,
        grouped=False)
    shuffle_write_step.inputs.append(input_pcoll)
    fused_stage.add_step(shuffle_write_step, origin_step=original_step)  # TODO: clarify role of origin_step argument.
    return fused_stage


  def _add_shuffle_finalize(self, shuffle_dataset_id, shuffle_write_stages):
    stage_name = 'S%02d' % self.next_stage_id
    self.next_stage_id += 1
    finalize_stage = ShuffleFinalizeStage(stage_name, self.execution_context, shuffle_dataset_id)
    self.execution_graph.add_stage(finalize_stage)
    for fused_stage in shuffle_write_stages:
      fused_stage.add_dependent(finalize_stage)
    return finalize_stage

  def _add_materialization_read(self,  original_step, read_step_name, element_coder, shuffle_dataset_id, finalize_stage):
    stage_name = 'S%02d' % self.next_stage_id
    self.next_stage_id += 1
    read_fused_stage = FusedStage(stage_name, self.execution_context)
    self.execution_graph.add_stage(read_fused_stage)
    read_step = ShuffleReadStep(read_step_name, element_coder, shuffle_dataset_id,
        LexicographicRange(LexicographicPosition.KEYSPACE_BEGINNING, LexicographicPosition.KEYSPACE_END), grouped=False)
    read_step.outputs[None] = PCollectionNode(read_step, None)
    read_fused_stage.add_step(read_step, origin_step=original_step)
    finalize_stage.add_dependent(read_fused_stage.input_watermark_node)
    return read_fused_stage




  def _process_flatten_step(self, original_step):
    # Note: we are careful to correctly handle the case where there are zero inputs to the Flatten.
    shuffle_dataset_id = self.next_shuffle_dataset_id
    self.next_shuffle_dataset_id += 1
    self.execution_context.shuffle_interface.create_dataset(shuffle_dataset_id)

    input_fused_stages = []
    for input_pcoll in original_step.inputs:
      # TODO: we will have multiple /Write materializes, potentially.  Should they have different names?
      fused_stage = self._add_materialization_write(original_step, original_step.name + '/FlWrite', input_pcoll, original_step.element_coder, shuffle_dataset_id)
      input_fused_stages.append(fused_stage)

    # Add finalization stage,.
    finalize_stage = self._add_shuffle_finalize(shuffle_dataset_id, input_fused_stages)

    # Add read stage, dependent on shuffle finalization.

    read_fused_stage = self._add_materialization_read(original_step, original_step.name + '/FlRead', original_step.element_coder, shuffle_dataset_id, finalize_stage)

    # Output stage is the new read stage.
    self.steps_to_fused_stages[original_step] = read_fused_stage




def generate_execution_graph(step_graph, execution_context):
  translator = ExecutionGraphTranslator(step_graph, execution_context)
  return translator.generate_execution_graph()
