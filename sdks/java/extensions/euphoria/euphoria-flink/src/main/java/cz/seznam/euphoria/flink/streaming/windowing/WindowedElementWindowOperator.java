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
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.flink.FlinkElement;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Objects;

/**
 * An {@link AbstractWindowOperator} implementation which expects input
 * elements to be of type {@link FlinkElement} and transforms these
 * into {@link KeyedMultiWindowedElement} on the fly.
 */
public class WindowedElementWindowOperator<KEY, WID extends Window>
        extends AbstractWindowOperator<FlinkElement<WID, ?>, KEY, WID> {

    MapFunction<FlinkElement<WID, ?>, KeyedMultiWindowedElement<WID, KEY, ?>> mapper;

    public WindowedElementWindowOperator(
            MapFunction<FlinkElement<WID, ?>, KeyedMultiWindowedElement<WID, KEY, ?>> mapper,
            Windowing<?, WID> windowing,
            StateFactory<?, State> stateFactory,
            CombinableReduceFunction<State> stateCombiner,
            boolean localMode) {
        super(windowing, stateFactory, stateCombiner, localMode);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    protected KeyedMultiWindowedElement<WID, KEY, ?>
    recordValue(StreamRecord<FlinkElement<WID, ?>> record) throws Exception {
        return mapper.map(record.getValue());
    }
}
