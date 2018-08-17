
package cz.seznam.euphoria.core.executor;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Interface for any executor.
 */
public interface Executor {

  /** Submit flow. Asynchronous operation. */
  Future<Integer> submit(Flow flow);

  /** Submit flow and wait for completion synchronously. */
  int waitForCompletion(Flow flow) throws Exception;

  /**
   * Operators that are considered to be basic and each executor has to
   * implement them.
   */
  @SuppressWarnings("unchecked")
  static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return (Set) Sets.newHashSet(
        FlatMap.class, Repartition.class, ReduceStateByKey.class, Union.class);
  }
  
}
