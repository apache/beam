package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Executor implementation using Apache Spark as a runtime.
 */
public class SparkExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutor.class);

  private final SparkConf conf;

  public SparkExecutor(SparkConf conf) {
    this.conf = conf;
  }

  public SparkExecutor() {
    this(new SparkConf());
  }

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int waitForCompletion(Flow flow) throws Exception {
    if (!isBoundedInput(flow)) {
      // FIXME this executor is intended to be used with bounded datasets only
      // but it works with unbounded as well for testing purposes
      LOG.warn("Input is type of unbounded source. Execution may not finish.");
      //throw new UnsupportedOperationException("Spark executor doesn't support unbounded input");
    }

    List<DataSink<?>> sinks = Collections.emptyList();
    try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {

      // FIXME blocking operation in Spark
      SparkFlowTranslator translator = new SparkFlowTranslator(sparkContext);
      sinks = translator.translateInto(flow);

    } catch (Exception e) {
      // FIXME in case of exception list of sinks will be empty
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

    return 0;
  }

  /**
   * Checks if the given {@link Flow} reads bounded inputs
   * @return {@code true} if all sources are bounded
   */
  protected boolean isBoundedInput(Flow flow) {
    // check if sources are bounded or not
    for (Dataset<?> ds : flow.sources()) {
      if (!ds.isBounded()) {
        return false;
      }
    }

    return true;
  }
}
