package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.translation.FlowTranslator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Executor implementation using Apache Flink as a runtime
 */
public class FlinkExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);

  private StreamExecutionEnvironment flinkStreamEnv;
  private boolean dumpExecPlan;

  public FlinkExecutor() {
    this(StreamExecutionEnvironment.getExecutionEnvironment());
  }

  public FlinkExecutor(StreamExecutionEnvironment flinkStreamEnv) {
    this.flinkStreamEnv = flinkStreamEnv;
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
    FlowTranslator translator = new FlowTranslator();
    List<DataSink<?>> sinks = translator.translateInto(flow, flinkStreamEnv);

    if (dumpExecPlan) {
      LOG.info("Flink execution plan for {}: {}",
          flow.getName(), flinkStreamEnv.getExecutionPlan());
    }

    try {
      flinkStreamEnv.execute(); // blocking operation
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
