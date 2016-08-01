package cz.seznam.euphoria.flink.translation;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.SingleInputOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink {@link DataStream}
 */
public class ExecutorContext {

  private final Map<Dataset<?>, DataStream<?>> datasetMapping
          = new IdentityHashMap<>();

  private final StreamExecutionEnvironment executionEnvironment;

  public ExecutorContext(StreamExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
  }

  /**
   * Retrieve list of Flink {@link DataStream} inputs of given operator
   */
  public List<DataStream<?>> getInputStreams(Operator<?, ?> operator) {
    List<Dataset<?>> inputDatasets = getInputs(operator);
    List<DataStream<?>> out = new ArrayList<>(inputDatasets.size());
    for (Dataset<?> dataset : inputDatasets) {
      DataStream ds = datasetMapping.get(dataset);
      if (ds == null) {
        throw new IllegalArgumentException("Matching DataStream missing for Dataset produced by "
                + dataset.getProducer().getName());
      }
      out.add(ds);
    }

    return out;
  }

  @SuppressWarnings("unchecked")
  public List<Dataset<?>> getInputs(Operator<?, ?> operator) {
    return new ArrayList<>((Collection) operator.listInputs());
  }

  public DataStream<?> getInputStream(SingleInputOperator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputStreams(operator));
  }

  public Dataset<?> getInput(SingleInputOperator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  public DataStream<?> getOutputStream(Operator<?, ?> operator) {
    DataStream<?> out = datasetMapping.get(operator.output());
    if (out == null) {
      throw new IllegalArgumentException("Output stream doesn't exists for operator " +
              operator.getName());
    }
    return out;
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

  public void setOutputStream(Operator<?, ?> operator, DataStream<?> output) {
    DataStream<?> prev = datasetMapping.put(operator.output(), output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
