package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.storage.FlinkReducingValueStorage;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

public class TriggerMergeContextWrapper
    extends TriggerContextWrapper
  implements TriggerContext.TriggerMergeContext {

  public TriggerMergeContextWrapper(Trigger.OnMergeContext flinkContext) {
    super(flinkContext);
  }

  @Override
  public void mergeStoredState(StorageDescriptorBase descriptor) {
    if (!(descriptor instanceof MergingStorageDescriptor)) {
      throw new IllegalStateException(
          "Storage descriptor '" + descriptor.getName() + "' must support merging!");
    }

    if (descriptor instanceof ValueStorageDescriptor.MergingValueStorageDescriptor) {
      ((Trigger.OnMergeContext) getFlinkContext())
          .mergePartitionedState(
              Descriptors.from((ValueStorageDescriptor.MergingValueStorageDescriptor) descriptor));
      return;
    }

    throw new UnsupportedOperationException(descriptor + " is not supported for merging yet!");
  }
}