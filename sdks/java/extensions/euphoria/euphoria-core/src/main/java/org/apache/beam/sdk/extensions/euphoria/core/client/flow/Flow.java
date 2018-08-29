/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.flow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Datasets;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ExtractEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A dependency graph of operators. */
@Audience(Audience.Type.CLIENT)
public class Flow implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Flow.class);

  /** Flow-wide settings. */
  private final Settings settings;

  /** An optional name of this flow. Typically used for debugging purposes. */
  private final String name;

  /** A map of {@link Operator} to its symbolic name. */
  private final Map<Operator<?, ?>, String> operatorNames = new HashMap<>();

  /** A list of all operators in the flow. The ordering is unspecified. */
  private final List<Operator<?, ?>> operators = new ArrayList<>();

  /** All outputs produced by all operators. */
  private final Set<Dataset<?>> outputs = new HashSet<>();

  /** All input datasets. */
  private final Set<Dataset<?>> sources = new HashSet<>();

  /** Map of datasets to consumers. */
  private final Map<Dataset<?>, Set<Operator<?, ?>>> datasetConsumers = new HashMap<>();

  protected Flow(@Nullable String name, Settings settings) {
    this.name = name == null ? "" : name;
    this.settings = cloneSettings(settings);
  }

  /**
   * Creates a new (anonymous) Flow.
   *
   * @return a new flow with an undefined name, i.e. either not named at all or with a system
   *     generated name
   */
  public static Flow create() {
    return create(null);
  }

  /**
   * Creates a new Flow.
   *
   * @param flowName a symbolic name of the flow; can be {@code null}
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
   * @return a newly created flow
   */
  public static Flow create(String flowName, Settings settings) {
    return new Flow(flowName, settings);
  }

  /**
   * Adds a new operator to the flow.
   *
   * @param <InputT> the type of elements the operator is consuming
   * @param <OutputT> the type of elements the operator is producing
   * @param <T> the type of the operator itself
   * @param operator the operator
   * @return instance of the operator
   */
  public <InputT, OutputT, T extends Operator<InputT, OutputT>> T add(T operator) {
    return add(operator, null);
  }

  /**
   * Called when a {@link Dataset} is persisted via {@link Dataset#persist}.
   *
   * @param <T> type parameter
   * @param dataset the dataset that was persisted
   */
  public <T> void onPersisted(Dataset<T> dataset) {
    // nop by default
  }

  /**
   * Adds a new operator to the flow.
   *
   * @param <InputT> the type of elements the operator is consuming
   * @param <OutputT> the type of elements the operator is producing
   * @param <T> the type of the operator itself
   * @param operator the operator to add to this flow.
   * @param logicalName the logical application specific name of the operator to be available for
   *     debugging purposes; can be {@code null}
   * @return the added operator
   */
  <InputT, OutputT, T extends Operator<InputT, OutputT>> T add(
      T operator, @Nullable String logicalName) {

    operatorNames.put(operator, buildOperatorName(operator, logicalName));
    operators.add(operator);
    outputs.add(operator.output());

    // ~ validate serialization
    validateSerializable(operator);

    // validate dependencies
    for (Dataset<InputT> d : operator.listInputs()) {
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

  private void validateSerializable(Operator o) {
    try {
      final CountingOutputStream outCounter = new CountingOutputStream();
      try (ObjectOutputStream out = new ObjectOutputStream(outCounter)) {
        out.writeObject(o);
      }
      LOG.debug(
          "Serialized operator {} ({}) into {} bytes",
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
   * @return the operator's symbolic name
   */
  String getOperatorName(Operator what) {
    return operatorNames.get(what);
  }

  /**
   * Retrieves a list of this flow's operators.
   *
   * <p>Note: this is *not* part of client API.
   *
   * @return an unmodifiable collection of this flow's operators (in no particular order)
   */
  public Collection<Operator<?, ?>> operators() {
    return Collections.unmodifiableList(operators);
  }

  /** @return a list of all sources associated with this flow */
  public Collection<Dataset<?>> sources() {
    return sources;
  }

  /** @return the count of operators in the flow */
  public int size() {
    return operators.size();
  }

  /**
   * Determines consumers of a given dataset assuming it is directly or indirectly associated with
   * this flow.
   *
   * @param dataset the data set to inspect
   * @return a collection of the currently registered consumers of the specified dataset
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
    return "Flow{" + "name='" + name + '\'' + ", size=" + size() + '}';
  }

  /** @return the settings associated with this this flow */
  public Settings getSettings() {
    return settings;
  }

  /**
   * Creates new input dataset.
   *
   * @param <T> the type of elements of the created input data set
   * @param source the data source to represent as a data set
   * @return a data set representing the specified source of data
   */
  public <T> Dataset<T> createInput(DataSource<T> source) {
    final Dataset<T> ret = Datasets.createInputFromSource(this, source);

    sources.add(ret);
    return ret;
  }

  /**
   * A convenience method to create a data set from the given source and assign the elements event
   * time using the user defined function.
   *
   * @param <T> the type of elements of the created input data set
   * @param source the data source to represent as a data set
   * @param evtTimeFn the user defined event time extraction function
   * @return a data set representing the specified source of data with assigned event time assigned
   */
  public <T> Dataset<T> createInput(DataSource<T> source, ExtractEventTime<T> evtTimeFn) {
    Dataset<T> input = createInput(source);
    return AssignEventTime.of(input).using(Objects.requireNonNull(evtTimeFn)).output();
  }

  private Settings cloneSettings(Settings settings) {
    return new Settings(settings);
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
}
