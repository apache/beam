package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.streaming.StreamingFlowTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Executor implementation using Apache Flink as a runtime
 */
public class FlinkExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);

  private final boolean localEnv;
  private boolean dumpExecPlan;

  public FlinkExecutor() {
    this(false);
  }

  public FlinkExecutor(boolean localEnv) {
    this.localEnv = localEnv;
  }

  /**
   * Specify whether to dump the flink execution plan before executing
   * a flow using {@link #waitForCompletion(Flow)}.
   */
  public void setDumpExecutionPlan(boolean dumpExecPlan) {
    this.dumpExecPlan = dumpExecPlan;
  }

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int waitForCompletion(Flow flow) throws Exception {
    ExecutionEnvironment.Mode mode = ExecutionEnvironment.determineMode(flow);

    LOG.info("Running flow in {} mode", mode);

    ExecutionEnvironment environment = new ExecutionEnvironment(mode, localEnv);
    FlowTranslator translator;
    if (mode == ExecutionEnvironment.Mode.BATCH) {
      translator = new BatchFlowTranslator(environment.getBatchEnv());
    } else {
      translator = new StreamingFlowTranslator(environment.getStreamEnv());
    }

    List<DataSink<?>> sinks = translator.translateInto(flow);

    if (dumpExecPlan) {
      LOG.info("Flink execution plan for {}: {}",
          flow.getName(), environment.dumpExecutionPlan());
    }

    try {
      environment.execute(); // blocking operation
    } catch (Exception e) {
      // when exception thrown rollback all sinks
      for (DataSink s : sinks) {
        try {
          s.rollback();
        } catch (Exception ex) {
          LOG.error("Exception during DataSink rollback", ex);
        }
      }
      throw e;
    }

    // when the execution is successful commit all sinks
    Exception ex = null;
    for (DataSink s : sinks) {
      try {
        s.commit();
      } catch (Exception e) {
        // save exception for later and try to commit rest of the sinks
        ex = e;
      }
    }

    // rethrow the exception if any
    if (ex != null) throw ex;

    return 0;
  }
}
