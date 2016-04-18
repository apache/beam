
package cz.seznam.euphoria.core.client.flow;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.InputPCollection;
import cz.seznam.euphoria.core.client.dataset.InputPStream;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.IORegistry;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A dependency graph of operators.
 */
public class Flow implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Flow.class);


  /** Flow-wide settings. */
  private final Settings settings;

  /**
   * An optional name of this flow. Typically used
   * for debugging purposes.
   */
  private final String name;


  /**
   * A map of {@link Operator} to its symbolic name.
   */
  private final Map<Operator<?, ?, ?>, String> operatorNames = new HashMap<>();


  /**
   * A list of all operators in the flow. The ordering is unspecified.
   */
  private final List<Operator<?, ?, ?>> operators = new ArrayList<>();


  /**
   * All outputs produced by all operators.
   */
  private final Set<Dataset<?>> outputs = new HashSet<>();


  /**
   * All input datasets.
   */
  private final Set<Dataset<?>> sources = new HashSet<>();


  private Flow(String name, Settings settings) {
    this.name = name == null ? "" : name;
    this.settings = settings;
  }


  /**
   * Creates a new Flow.
   */
  public static Flow create(String flowName) {
    return new Flow(flowName, new Settings());
  }


  /**
   * Creates a new Flow.
   */
  public static Flow create(String flowName, Settings settings) {
    return new Flow(flowName, settings);
  }



  /**
   * Adds a new operator to the flow.
   *
   * @param operator the operator (e.g. {@link Group})
   *
   * @return instance of the operator
   */
  public <IN, OUT, TYPE extends Dataset<OUT>, T extends Operator<IN, OUT, TYPE>> T add(
      T operator) {
    return add(operator, null);
  }

  /**
   * Adds a new operator to the flow.
   *
   * @param operator the operator to add to this flow.
   * @param logicalName the logical application specific name of the operator
   *          to be available for debugging purposes; can be {@code null}
   *
   * @return the added operator
   */
  <IN, OUT, TYPE extends Dataset<OUT>, T extends Operator<IN, OUT, TYPE>> T add(
      T operator, String logicalName) {
    operatorNames.put(operator, buildOperatorName(operator, logicalName));
    operators.add(operator);
    outputs.add(operator.output());

    // ~ validate serialization
    validateSerializable(operator);

    // validate dependencies
    for (Dataset<IN> d : operator.listInputs()) {
      if (!sources.contains(d) && !outputs.contains(d))
        throw new IllegalArgumentException(
                "Invalid input: All dependencies must already be present in the flow!");
    }
    return operator;
  }

  private void validateSerializable(Operator o) {
    try {
      final CountingOutputStream outCounter =
              new CountingOutputStream(new NullOutputStream());
      try (ObjectOutputStream out = new ObjectOutputStream(outCounter)) {
        out.writeObject(o);
      }
      LOG.debug("Serialized operator {} ({}) into {} bytes",
              new Object[] {o.toString(), o.getClass(), outCounter.getByteCount()});
    } catch (IOException e) {
      throw new IllegalStateException("Operator " + o + " not serializable!", e);
    }
  }

  private String buildOperatorName(Operator op, String logicalName) {
    StringBuilder sb = new StringBuilder(64);
    sb.append(op.getName()).append('@').append(operatorNames.size() + 1);
    logicalName = Util.trimToNull(logicalName);
    if (logicalName != null) {
      sb.append('#').append(logicalName);
    }
    return sb.toString();
  }

  /**
   * Retrieve a symbolic name of operator within the flow.
   */
  String getOperatorName(Operator what) {
    return operatorNames.get(what);
  }


  /**
   * Retrieves a list of this flow's operators.
   * <p />
   * Note: this is *not* part of client API.
   */
  public Collection<Operator<?, ?, ?>> operators() {
    return Collections.unmodifiableList(operators);
  }


  /** Retrieve list of all sources. */
  public Collection<Dataset<?>> sources() {
    return sources;
  }


  /**
   * Retrieve count of operators in the flow.
   */
  public int size() {
    return operators.size();
  }


  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Flow{" +
            "name='" + name + '\'' +
            ", size=" + size() +
            '}';
  }


  /** Retrieve settings of this flow. */
  public Settings getSettings() {
    return settings;
  }

  /**
   * Create new input dataset.
   */
  public <T> Dataset<T> createInput(URI uri) throws Exception {
    final DataSource<T> source = getSourceFromURI(uri);
    final Dataset<T> ret;

    if (source.isBounded()) {
      ret = new InputPCollection<T>(this, source) {

        @Override
        public <X> Partitioning<X> getPartitioning()
        {
          return new Partitioning<X>() {

            @Override
            public Partitioner<X> getPartitioner()
            {
              return new HashPartitioner<>();
            }

            @Override
            public int getNumPartitions()
            {
              return source.getPartitions().size();
            }

          };
        }
      };
    } else {
     ret = new InputPStream<T>(this, source) {

      @Override
      public <X> Partitioning<X> getPartitioning()
      {
        return new Partitioning<X>() {

          @Override
          public Partitioner<X> getPartitioner()
          {
            return new HashPartitioner<>();
          }

          @Override
          public int getNumPartitions()
          {
            return source.getPartitions().size();
          }

        };
      }

     };
    }

    sources.add(ret);
    return ret;
  }

  /**
   * Create batch input. This will fail if the provided URI doesn't represent
   * bounded input.
   */
  public <T> PCollection<T> createBatchInput(URI uri) throws Exception {
    Dataset<T> ret = createInput(uri);
    if (!ret.isBounded()) {
      throw new IllegalArgumentException(
          "Cannot create batch input from given URI: " + uri);
    }
    return (PCollection<T>) ret;
  }

  public <T> DataSink<T> createOutput(URI uri) throws Exception {
    return getSinkFromURI(uri);
  }

  private <T> DataSource<T> getSourceFromURI(URI uri) throws Exception {
    IORegistry registry = IORegistry.get(settings);
    return registry.openSource(uri, settings);
  }

  private <T> DataSink<T> getSinkFromURI(URI uri) throws Exception {
    IORegistry registry = IORegistry.get(settings);
    return registry.openSink(uri, settings);
  }
}
