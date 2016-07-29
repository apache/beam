package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.SingleInputOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink {@link DataStream}
 */
class TranslationContext {

  private final Map<Dataset<?>, DataStream<?>> datasetMapping
          = new IdentityHashMap<>();

  private final StreamExecutionEnvironment executionEnvironment;

  public TranslationContext(StreamExecutionEnvironment executionEnvironment) {
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
        throw new NullPointerException("Matching DataStream missing for Dataset produced by "
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
    return getInputStreams(operator).get(0);
  }

  public Dataset<?> getInput(SingleInputOperator<?, ?> operator) {
    return getInputs(operator).get(0);
  }

  public DataStream<?> getOutputStream(Operator<?, ?> operator) {
    return datasetMapping.get(operator.output());
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
