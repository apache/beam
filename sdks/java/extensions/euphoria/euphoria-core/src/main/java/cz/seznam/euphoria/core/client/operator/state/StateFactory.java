/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import cz.seznam.euphoria.core.client.io.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Factory for states.
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface StateFactory<IN, OUT, STATE extends State<IN, OUT>> extends Serializable {

  /**
   * Factory method to create new state instances.
   *
   * @param stateContext context provided by the executor
   * @param context a context allowing the newly created state for the
   *                duration of its existence to emit output elements
   *
   * @return a newly created state
   */
  STATE createState(StateContext stateContext,
                    @Experimental("https://github.com/seznam/euphoria/issues/118")
                    @Nullable Collector<OUT> context);

}
