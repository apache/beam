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
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;

/**
 * A dataset registered in flow.
 */
public abstract class FlowAwareDataset<T> implements Dataset<T> {

  private final Flow flow;

  public FlowAwareDataset(Flow flow) {
    this.flow = flow;
  }

  @Override
  public Flow getFlow() {
    return flow;
  }

  @Override
  public Collection<Operator<?, ?>> getConsumers() {
    return flow.getConsumersOf(this);
  }


}
