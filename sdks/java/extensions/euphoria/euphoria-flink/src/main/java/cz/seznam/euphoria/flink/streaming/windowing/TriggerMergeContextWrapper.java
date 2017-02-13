/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.flink.storage.Descriptors;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

public class TriggerMergeContextWrapper
    extends TriggerContextWrapper
  implements TriggerContext.TriggerMergeContext {

  public TriggerMergeContextWrapper(Trigger.OnMergeContext flinkContext) {
    super(flinkContext);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void mergeStoredState(StorageDescriptor descriptor) {
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