

import itertools

from apache_beam.internal import pickler

from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.pvalue import PBegin
from apache_beam.runners.worker import operation_specs

from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP

# TODO: move part of this to maybe steps.py?

class StepGraph(object):
  def __init__(self):
    self.steps = []  # set?

    # The below are used for conevenience in construction of the StepGraph, but
    # not by any subsequent logic.
    self.transform_node_to_step = {}
    self.pcollection_to_node = {}

  # def register_origin_transform_node()
  def add_step(self, transform_node, step):
    self.steps.append(step)
    self.transform_node_to_step[transform_node] = step
    print 'TRANSFORM_NODE', transform_node
    print 'inputs', transform_node.inputs
    inputs = []
    for input_pcollection in transform_node.inputs:
      if isinstance(input_pcollection, PBegin):
        continue
      assert input_pcollection in self.pcollection_to_node
      pcollection_node = self.pcollection_to_node[input_pcollection]
      pcollection_node.add_consumer(step)
      inputs.append(pcollection_node)
    step.inputs = inputs
    print 'YO inputs', inputs

    # TODO: side inputs in this graph.
    side_inputs = []
    for input_pcollection_view in transform_node.side_inputs:
      input_pcollection = input_pcollection_view.pvalue
      print 'input "PCOLLECTION"', input_pcollection
      assert input_pcollection in self.pcollection_to_node
      pcollection_node = self.pcollection_to_node[input_pcollection]
      pcollection_node.add_side_input_consumer(step)  # TODO: necessary?
      side_inputs.append(pcollection_node)
    step.side_inputs = side_inputs

    outputs = {}
    for tag, output_pcollection in transform_node.outputs.iteritems():
      print 'TAG', tag, output_pcollection
      # TODO: do we want to associate system names here or somewhere?  might be useful for association with monitoring and such.
      pcollection_node = PCollectionNode(step, tag)
      self.pcollection_to_node[output_pcollection] = pcollection_node
      outputs[tag] = pcollection_node
    step.outputs = outputs
    print 'YO outputs', outputs
    # if original_node:  # original_transform_node?
    #   self.transform_node_to_step[original_node] = step
    # if input_step:  # TODO: should this be main_input?
    #   step._add_input(input_step)
    #   input_step._add_output(step)

  def get_step_from_node(self, transform_node):
    return self.transform_node_to_step[transform_node]

  def __repr__(self):
    return 'StepGraph(steps=%s)' % self.steps


class PCollectionNode(object):
  def __init__(self, step, tag):
    super(PCollectionNode, self).__init__()
    self.step = step
    self.tag = tag
    self.consumers = []
    self.side_input_consumers = []

  def add_consumer(self, consumer_step):
    self.consumers.append(consumer_step)

  def add_side_input_consumer(self, consumer_step):
    # TODO: to what extent is this useful as tracking separate from self.consumers?
    self.side_input_consumers.append(consumer_step)

  def __repr__(self):
    return 'PCollectionNode[%s.%s]' % (self.step.name, self.tag)


class WatermarkNode(object):
  def __init__(self):
    super(WatermarkNode, self).__init__()
    self.input_watermark = MIN_TIMESTAMP
    self.watermark_hold = MAX_TIMESTAMP
    self.output_watermark = MIN_TIMESTAMP
    self.watermark_parents = []
    self.watermark_children = []

  def add_dependent(self, dependent):
    actual_dependent = dependent
    if isinstance(dependent, CompositeWatermarkNode):
      actual_dependent = dependent.input_watermark_node
    self.watermark_children.append(actual_dependent)
    actual_dependent.watermark_parents.append(self)

  def _refresh_input_watermark(self):
    print '_refresh_input_watermark OLD', self, self.input_watermark
    new_input_watermark = MAX_TIMESTAMP
    if self.watermark_parents:
      new_input_watermark = min(parent.output_watermark for parent in self.watermark_parents)
    print '_refresh_input_watermark NEW', self, new_input_watermark
    if new_input_watermark > self.input_watermark:
      self._advance_input_watermark(new_input_watermark)

  def _refresh_output_watermark(self):
    new_output_watermark = min(self.input_watermark, self.watermark_hold)
    if new_output_watermark > self.output_watermark:
      print 'OUTPUT watermark advanced', self, new_output_watermark
      self.output_watermark = new_output_watermark
      for dependent in self.watermark_children:
        print 'CHECK', dependent
        dependent._refresh_input_watermark()
    else:
      print 'OUTPUT watermark unchanged', self, self.output_watermark

  def set_watermark_hold(self, hold_time=None):
    # TODO: do we need some synchronization?
    if hold_time is None:
      hold_time = MAX_TIMESTAMP
    self.watermark_hold = hold_time
    self._refresh_output_watermark()

  def _advance_input_watermark(self, new_watermark):
    if new_watermark <= self.input_watermark:
      print 'not advancing input watermark', self
      return
    self.input_watermark = new_watermark
    print 'advancing input watermark', self
    self.input_watermark_advanced(new_watermark)
    self._refresh_output_watermark()



  def input_watermark_advanced(self, new_watermark):
    pass



# class StepInfo(object):
#   def __init__(self, input_pcollection, output_pcollections):
#     self.input_pcollection = input_pcollection
#     self.output_pcollections = output_pcollections

class Step(WatermarkNode):
  def __init__(self, name):
    super(Step, self).__init__()
    self.name = name
    self.inputs = []  # Should have one element except in case of Combine
    self.side_inputs = []
    self.outputs = {}

  # def _add_input(self, input_step):
  #   assert isinstance(input_step, Step)
  #   self.inputs.append(input_step)

  # def _add_output(self, output_step):  # add_consumer? what happens with named outputs? do we care?
  #   assert isinstance(output_step, Step)
  #   self.outputs.append(output_step)

  def has_predecessor_step(self, step):
    if self is step:
      return True
    for pcoll_node in itertools.chain(self.inputs, self.side_inputs):
      if pcoll_node.step.has_predecessor_step(step):
        return True
    return False


  def copy(self):
    """Return copy of this Step, without its attached inputs or outputs."""
    raise NotImplementedError()

  def __repr__(self):
    return 'Step(%s, coder: %s)' % (self.name, getattr(self, 'element_coder', None))

class ReadStep(Step):
  def __init__(self, name, original_source_bundle, element_coder):
    super(ReadStep, self).__init__(name)
    self.original_source_bundle = original_source_bundle
    self.element_coder = element_coder

  def copy(self):
    return ReadStep(self.name, self.original_source_bundle, self.element_coder)

  def as_operation(self, unused_step_index):
    return operation_specs.WorkerRead(self.original_source_bundle, output_coders=[self.element_coder])


class ParDoFnData(object):
  def __init__(self, fn, args, kwargs, si_tags_and_types, windowing):
    self.fn = fn
    self.args = args
    self.kwargs = kwargs
    self.si_tags_and_types = si_tags_and_types
    self.windowing = windowing

  def __repr__(self):
    return 'ParDoFnData(fn: %s, args: %s, kwargs: %s, si_tags_and_types: %s, windowing: %s)' % (
        self.fn, self.args, self.kwargs, self.si_tags_and_types, self.windowing
      )

  def as_serialized_fn(self):
    return pickler.dumps((self.fn, self.args, self.kwargs, self.si_tags_and_types, self.windowing))


class ParDoStep(Step):
  def __init__(self, name, pardo_fn_data, element_coder, output_tags):
    super(ParDoStep, self).__init__(name)
    self.pardo_fn_data = pardo_fn_data
    self.element_coder = element_coder
    self.output_tags = output_tags
    self.side_input_sources = []

  def copy(self):
    return ParDoStep(self.name, self.pardo_fn_data, self.element_coder, self.output_tags)

  def as_operation(self, step_index):  # TODO: rename to step_indices throughout?
    assert len(self.inputs) == 1
    return operation_specs.WorkerDoFn(
        serialized_fn=self.pardo_fn_data.as_serialized_fn(),
        output_tags=[PropertyNames.OUT] + ['%s_%s' % (PropertyNames.OUT, tag)
                                           for tag in self.output_tags],
        output_coders=[self.element_coder] * (len(self.output_tags) + 1),
        input=(step_index[self.inputs[0].step], 0),  # TODO: multiple output support.
        side_inputs=self.side_input_sources)

class GroupByKeyOnlyStep(Step):
  def __init__(self, name, shuffle_coder, output_coder):
    super(GroupByKeyOnlyStep, self).__init__(name)
    self.shuffle_coder = shuffle_coder
    self.output_coder = output_coder

  def copy(self):
    return GroupByKeyOnlyStep(self.name, self.shuffle_coder, self.output_coder)


class ShuffleWriteStep(Step):
  def __init__(self, name, element_coder, dataset_id, grouped=True):
    super(ShuffleWriteStep, self).__init__(name)
    self.element_coder = element_coder
    self.dataset_id = dataset_id
    self.grouped = grouped

  def copy(self):
    return ShuffleWriteStep(self.name, self.element_coder, self.dataset_id, grouped=self.grouped)

  def as_operation(self, step_index):
    return operation_specs.LaserShuffleWrite(
        dataset_id=self.dataset_id,
        transaction_id=-1,
        output_coders=[self.element_coder],
        input=(step_index[self.inputs[0].step], 0),  # TODO: multiple output support.
        grouped=self.grouped,
        )



class ShuffleReadStep(Step):
  def __init__(self, name, element_coder, dataset_id, key_range, grouped=True):
    super(ShuffleReadStep, self).__init__(name)
    self.element_coder = element_coder
    self.dataset_id = dataset_id
    self.key_range = key_range
    self.grouped = grouped

  def copy(self):
    return ShuffleReadStep(self.name, self.element_coder, self.dataset_id, self.key_range, grouped=self.grouped)

  def as_operation(self, step_index):
    return operation_specs.LaserShuffleRead(
        dataset_id=self.dataset_id,
        key_range=self.key_range,
        output_coders=[self.element_coder],
        grouped=self.grouped,
        )


class FlattenStep(Step):
  def __init__(self, name, element_coder):
    super(FlattenStep, self).__init__(name)
    self.element_coder = element_coder

  def copy(self):
    return FlattenStep(self.name, self.element_coder)


class CompositeWatermarkNode(WatermarkNode):
  class InputWatermarkNode(WatermarkNode):
    def __init__(self, composite_node):
      super(CompositeWatermarkNode.InputWatermarkNode, self).__init__()
      self.composite_node = composite_node

    def input_watermark_advanced(self, new_watermark):
      self.composite_node.input_watermark_advanced(new_watermark)

  class OutputWatermarkNode(WatermarkNode):
    def __init__(self, composite_node):
      super(CompositeWatermarkNode.OutputWatermarkNode, self).__init__()

  def __init__(self):
    super(CompositeWatermarkNode, self).__init__()
    self.input_watermark_node = CompositeWatermarkNode.InputWatermarkNode(self)
    self.output_watermark_node = CompositeWatermarkNode.OutputWatermarkNode(self)

  def add_dependent(self, dependent):
    self.output_watermark_node.add_dependent(dependent)

  # def set_watermark_hold(self, hold_time=None):  # TODO: should we remove this so it is more explicit?
  #   self.input_watermark_node.set_watermark_hold(hold_time=hold_time)
