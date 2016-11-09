
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for any executor.
 */
public interface Executor {

  /** 
   * Submit flow as a job. Asynchronous operation.
   * @param flow {@link Flow} to be submitted
   * @return future of the job's execution
   */
  CompletableFuture<Result> submit(Flow flow);
  
  /**
   * Cancel all executions.
   */
  void shutdown();

  /**
   * Operators that are considered to be basic and each executor has to
   * implement them.
   * @return set of basic operators
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return (Set) Sets.newHashSet(
        FlatMap.class, Repartition.class, ReduceStateByKey.class, Union.class);
  }
  
  /**
   * Execution (job) result. Should contain aggregators, etc.
   */
  public static class Result {
    
    private final int returnCode;
    private final Throwable t;

    /**
     * @param returnCode final status of the execution
     * @param t exception if any
     */
    public Result(int returnCode, Throwable t) {
      this.returnCode = returnCode;
      this.t = t;
    }
    
    /**
     * @return final status of the execution
     */
    public int getReturnCode() {
      return returnCode;
    }
    
    /**
     * @return potential exception thrown during execution that canceled the execution
     */
    public Optional<Throwable> getException() {
      return Optional.ofNullable(t);
    }
  }
}
