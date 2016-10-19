package cz.seznam.euphoria.flink.streaming.windowing;


import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

/**
 * Adapts Flink {@link Trigger.TriggerContext} to be used as trigger context in
 * Euphoria API
 */
public class TriggerContextWrapper implements TriggerContext {

  private final Trigger.TriggerContext flinkContext;

  public TriggerContextWrapper(Trigger.TriggerContext flinkContext) {
    this.flinkContext = flinkContext;
  }

  @Override
  public boolean registerTimer(long stamp, Window window) {
    if (stamp <= getCurrentTimestamp()) {
      return false;
    }

    flinkContext.registerEventTimeTimer(stamp);
    return true;
  }

  @Override
  public void deleteTimer(long stamp, Window window) {
    flinkContext.deleteEventTimeTimer(stamp);
  }

  @Override
  public long getCurrentTimestamp() {
    return flinkContext.getCurrentWatermark();
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new FlinkValueStorage<>(flinkContext.getPartitionedState(
            new ValueStateDescriptor<>(
                    descriptor.getName(),
                    descriptor.getValueClass(),
                    descriptor.getDefaultValue())));
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FlinkListStorage<>(flinkContext.getPartitionedState(
            new ListStateDescriptor<>(
                    descriptor.getName(),
                    descriptor.getElementClass())));
  }
}
