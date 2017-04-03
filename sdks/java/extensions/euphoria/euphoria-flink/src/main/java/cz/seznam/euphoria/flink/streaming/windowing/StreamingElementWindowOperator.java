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

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Objects;

/**
 * An {@link AbstractWindowOperator} implementation which expects input
 * elements to be of type {@link StreamingElement} and transforms these
 * into {@link KeyedMultiWindowedElement} on the fly.
 */
public class StreamingElementWindowOperator<KEY, WID extends Window>
        extends AbstractWindowOperator<StreamingElement<WID, ?>, KEY, WID> {

  WindowAssigner<?, KEY, ?, WID> windowAssigner;

  public StreamingElementWindowOperator(
          WindowAssigner<?, KEY, ?, WID> windowAssigner,
          Windowing<?, WID> windowing,
          StateFactory<?, State> stateFactory,
          CombinableReduceFunction<State> stateCombiner,
          boolean localMode,
          int descriptorsCacheMaxSize) {
    super(windowing, stateFactory, stateCombiner, localMode, descriptorsCacheMaxSize);
    this.windowAssigner = Objects.requireNonNull(windowAssigner);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected KeyedMultiWindowedElement<WID, KEY, ?>
  recordValue(StreamRecord<StreamingElement<WID, ?>> record) throws Exception {
    return windowAssigner.apply((StreamRecord) record);
  }
}
