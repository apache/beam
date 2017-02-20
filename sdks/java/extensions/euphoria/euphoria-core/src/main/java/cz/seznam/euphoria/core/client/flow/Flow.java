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
package cz.seznam.euphoria.core.client.flow;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Datasets;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.IORegistry;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.util.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
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
  private final Map<Operator<?, ?>, String> operatorNames = new HashMap<>();


  /**
   * A list of all operators in the flow. The ordering is unspecified.
   */
  private final List<Operator<?, ?>> operators = new ArrayList<>();


  /**
   * All outputs produced by all operators.
   */
  private final Set<Dataset<?>> outputs = new HashSet<>();


  /**
   * All input datasets.
   */
  private final Set<Dataset<?>> sources = new HashSet<>();


  /**
   * Map of datasets to consumers.
   */
  private final Map<Dataset<?>, Set<Operator<?, ?>>> datasetConsumers
      = new HashMap<>();


  private Flow(@Nullable String name, Settings settings) {
    this.name = name == null ? "" : name;
    this.settings = cloneSettings(settings);
  }

  /**
   * Creates a new (anonymous) Flow.
   *
   * @return a new flow with an undefined name,
   *          i.e. either not named at all or with a system generated name
   */
  public static Flow create() {
    return create(null);
  }

  /**
   * Creates a new Flow.
   *
   * @param flowName a symbolic name of the flow; can be {@code null}
   *
   * @return a newly created flow
   */
  public static Flow create(@Nullable String flowName) {
    return new Flow(flowName, new Settings());
  }


  /**
   * Creates a new Flow.
   *
   * @param flowName a symbolic name of the flow; can be {@code null}
   * @param settings euphoria settings to be associated with the new flow
   *
   * @return a newly created flow
   */
  public static Flow create(String flowName, Settings settings) {
    return new Flow(flowName, settings);
  }



  /**
   * Adds a new operator to the flow.
   *
   * @param <IN> the type of elements the operator is consuming
   * @param <OUT> the type of elements the operator is producing
   * @param <T> the type of the operator itself
   *
   * @param operator the operator
   *
   * @return instance of the operator
   */
  public <IN, OUT, T extends Operator<IN, OUT>> T add(T operator) {
    return add(operator, null);
  }

  /**
   * Adds a new operator to the flow.
   *
   * @param <IN> the type of elements the operator is consuming
   * @param <OUT> the type of elements the operator is producing
   * @param <T> the type of the operator itself
   *
   * @param operator the operator to add to this flow.
   * @param logicalName the logical application specific name of the operator
   *          to be available for debugging purposes; can be {@code null}
   *
   * @return the added operator
   */
  <IN, OUT, T extends Operator<IN, OUT>> T add(T operator, @Nullable String logicalName) {

    operatorNames.put(operator, buildOperatorName(operator, logicalName));
    operators.add(operator);
    outputs.add(operator.output());

    // ~ validate serialization
    validateSerializable(operator);

    // validate dependencies
    for (Dataset<IN> d : operator.listInputs()) {
      if (!sources.contains(d) && !outputs.contains(d)) {
        throw new IllegalArgumentException(
            "Invalid input: All dependencies must already be present in the flow!");
      }
      Set<Operator<?, ?>> consumers = this.datasetConsumers.get(d);
      if (consumers == null) {
        consumers = new HashSet<>();
        this.datasetConsumers.put(d, consumers);
      }
      consumers.add(operator);
    }
    return operator;
  }

  // ~ counts the number of bytes written through the
  // stream while discarding the data to be written
  static class CountingOutputStream extends OutputStream {
    long count;

    @Override
    public void write(int b) throws IOException {
      count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      count += len;
    }
  }

  private void validateSerializable(Operator o) {
    try {
      final CountingOutputStream outCounter = new CountingOutputStream();
      try (ObjectOutputStream out = new ObjectOutputStream(outCounter)) {
        out.writeObject(o);
      }
      LOG.debug("Serialized operator {} ({}) into {} bytes",
              new Object[] {o.toString(), o.getClass(), outCounter.count});
    } catch (IOException e) {
      throw new IllegalStateException("Operator " + o + " not serializable!", e);
    }
  }

  private String buildOperatorName(Operator op, @Nullable String logicalName) {
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
   *
   * @param what the operator whose symbolic name to retrieve within this flow
   *
   * @return the operator's symbolic name
   */
  String getOperatorName(Operator what) {
    return operatorNames.get(what);
  }


  /**
   * Retrieves a list of this flow's operators.
   * <p>
   * Note: this is *not* part of client API.
   *
   * @return an unmodifiable collection of this flow's operators
   *          (in no particular order)
   */
  public Collection<Operator<?, ?>> operators() {
    return Collections.unmodifiableList(operators);
  }


  /**
   * @return a list of all sources associated with this flow
   */
  public Collection<Dataset<?>> sources() {
    return sources;
  }


  /**
   * @return the count of operators in the flow
   */
  public int size() {
    return operators.size();
  }

  /**
   * Determines consumers of a given dataset assuming it is directly
   * or indirectly associated with this flow.
   *
   * @param dataset the data set to inspect
   *
   * @return a collection of the currently registered consumers of
   *          the specified dataset
   */
  public Collection<Operator<?, ?>> getConsumersOf(Dataset<?> dataset) {
    Set<Operator<?, ?>> consumers = this.datasetConsumers.get(dataset);
    if (consumers != null) {
      return consumers;
    }
    return new ArrayList<>();
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


  /** @return the settings associated with this this flow */
  public Settings getSettings() {
    return settings;
  }

  /**
   * Creates a new input/source data set based on the specified URI.
   *
   * @param <T> the type of elements extracted form the source; this is not
   *          type-safe. if the caller mixes up the source and the expected type
   *          from such a source the result may be {@link ClassCastException}s at
   *          later points in time
   *
   * @param uri the URI describing the source of the dataset
   *
   * @return a dataset representing the specified source
   *
   * @throws Exception if setting up the source fails for some reason
   *
   * @see IORegistry
   */
  public <T> Dataset<T> createInput(URI uri) throws Exception {
    return createInput(getSourceFromURI(uri));
  }

  /**
   * Creates new input dataset.
   *
   * @param <T> the type of elements of the created input data set
   *
   * @param source the data source to represent as a data set
   *
   * @return a data set representing the specified source of data
   */
  public <T> Dataset<T> createInput(DataSource<T> source) {
    final Dataset<T> ret = Datasets.createInputFromSource(this, source);

    sources.add(ret);
    return ret;
  }

  /**
   * Creates a new output/sink data set based on the specified URI.
   *
   * @param <T> the type of elements being written to the sink; this is
   *         not type-safe. if the caller mixes up the sink and the expected
   *         type of a such a sink the result may be {@link ClassCastException}s
   *         at later points in time
   *
   * @param uri the URI describing the sink of the data set
   *
   * @return a data sink based on the specified URI
   *
   * @throws Exception if setting up the sink fails for some reason
   *
   * @see IORegistry
   */
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

  private Settings cloneSettings(Settings settings) {
    return new Settings(settings);
  }
}
