/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.executor.FlowUnfolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Translates given {@link Flow} into Flink execution environment
 */
public abstract class FlowTranslator {

  /**
   * A functor to accept operators for translation if the  operator's
   * type equals a specified, fixed type. An optional custom "accept"
   * function can be provided to further tweak the decision whether
   * a particular operator instance is to be accepted for translation
   * or not.
   *
   * @param <O> the fixed operator type accepted
   */
  public static final class TranslateAcceptor<O>
      implements UnaryPredicate<Operator<?, ?>> {

    final Class<O> type;
    final UnaryPredicate<O> accept;

    public TranslateAcceptor(Class<O> type) {
      this (type, null);
    }

    public TranslateAcceptor(Class<O> type, UnaryPredicate<O> accept) {
      this.type = Objects.requireNonNull(type);
      this.accept = accept;
    }

    @Override
    public Boolean apply(Operator<?, ?> operator) {
      return type == operator.getClass()
          && (accept == null || accept.apply(type.cast(operator)));
    }
  }

  /**
   * Translates given flow to Flink execution environment
   * @return List of {@link DataSink} processed in given flow (leaf nodes)
   */
  @SuppressWarnings("unchecked")
  public abstract List<DataSink<?>> translateInto(Flow flow);

  /**
   * Converts {@link Flow} to {@link DAG} of Flink specific {@link FlinkOperator}.
   *
   * <p>Invokes {@link #getAcceptors()} to determine which user provided
   * operators to accept for direct translation, i.e. which to leave in
   * the resulting DAG without expanding them to their {@link Operator#getBasicOps()}.
   */
  protected DAG<FlinkOperator<?>> flowToDag(Flow flow) {
    // ~ get acceptors for translation
    Map<Class, Collection<TranslateAcceptor>> acceptors =
        buildAcceptorsIndex(getAcceptors());
    // ~ now, unfold the flow based on the specified acceptors
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, operator -> {
      // accept the operator if any of the specified acceptors says so
      Collection<TranslateAcceptor> accs = acceptors.get(operator.getClass());
      if (accs != null && !accs.isEmpty()) {
        for (TranslateAcceptor acc : accs) {
          if (acc.apply(operator)) {
            return true;
          }
        }
      }
      return false;
    });
    // ~ turn the generic operator graph into flink specified graph
    return createOptimizer().optimize(unfolded);
  }

  /**
   * Helper method to build an index over the given acceptors by
   * {@link TranslateAcceptor#type}.
   */
  private Map<Class, Collection<TranslateAcceptor>>
  buildAcceptorsIndex(Collection<TranslateAcceptor> accs) {
    IdentityHashMap<Class, Collection<TranslateAcceptor>> idx =
        new IdentityHashMap<>(accs.size());
    for (TranslateAcceptor<?> acc : accs) {
      Collection<TranslateAcceptor> cac =
          idx.computeIfAbsent(acc.type, k -> new ArrayList<>());
      cac.add(acc);
    }
    return idx;
  }

  protected FlowOptimizer createOptimizer() {
    return new FlowOptimizer();
  }

  protected abstract Collection<TranslateAcceptor> getAcceptors();
}
