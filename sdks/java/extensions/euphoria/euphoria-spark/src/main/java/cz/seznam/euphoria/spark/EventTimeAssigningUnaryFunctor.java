/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;

import java.util.Objects;

class EventTimeAssigningUnaryFunctor<WID extends Window, IN, OUT>
    extends UnaryFunctorWrapper<WID, IN, OUT> {

  private final ExtractEventTime<IN> evtTimeFn;

  EventTimeAssigningUnaryFunctor(UnaryFunctor<IN, OUT> functor,
                                 ExtractEventTime<IN> evtTimeFn) {
    super(functor);
    this.evtTimeFn = Objects.requireNonNull(evtTimeFn);
  }

  @Override
  protected long getTimestamp(SparkElement<WID, IN> elem) {
    return evtTimeFn.extractTimestamp(elem.getElement());
  }
}
