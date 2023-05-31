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
package org.apache.beam.sdk.io.cdap.context;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Class for Batch, Sink and Stream CDAP wrapper classes that use it to provide common details. */
@SuppressWarnings({"TypeParameterUnusedInFormals", "nullness"})
public abstract class BatchContextImpl implements BatchContext {

  public static final String DEFAULT_SCHEMA_FIELD_NAME = "Name";
  public static final String DEFAULT_SCHEMA_RECORD_NAME = "Record";

  private final FailureCollectorWrapper failureCollector = new FailureCollectorWrapper();

  /**
   * This should be set after {@link SubmitterLifecycle#prepareRun(Object)} call with passing this
   * context object as a param.
   */
  protected InputFormatProvider inputFormatProvider;

  /**
   * This should be set after {@link SubmitterLifecycle#prepareRun(Object)} call with passing this
   * context object as a param.
   */
  protected OutputFormatProvider outputFormatProvider;

  /**
   * This should be set after {@link SubmitterLifecycle#prepareRun(Object)} call with passing this
   * context object as a param.
   */
  protected Map<String, String> settableArguments = new HashMap<>();

  private final Timestamp startTime = new Timestamp(System.currentTimeMillis());

  public InputFormatProvider getInputFormatProvider() {
    return inputFormatProvider;
  }

  public OutputFormatProvider getOutputFormatProvider() {
    return outputFormatProvider;
  }

  @Override
  public String getStageName() {
    return null;
  }

  @Override
  public String getNamespace() {
    return null;
  }

  @Override
  public String getPipelineName() {
    return null;
  }

  @Override
  public long getLogicalStartTime() {
    return this.startTime.getTime();
  }

  @Override
  public StageMetrics getMetrics() {
    return null;
  }

  @Override
  public PluginProperties getPluginProperties() {
    return null;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Override
  public Schema getInputSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(DEFAULT_SCHEMA_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(DEFAULT_SCHEMA_RECORD_NAME, fields);
  }

  @Override
  public @Nullable Map<String, Schema> getInputSchemas() {
    return null;
  }

  @Override
  public Schema getOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of(DEFAULT_SCHEMA_FIELD_NAME, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(DEFAULT_SCHEMA_RECORD_NAME, fields);
  }

  @Override
  public Map<String, Schema> getOutputPortSchemas() {
    return null;
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties)
      throws DatasetManagementException {}

  @Override
  public boolean datasetExists(String datasetName) throws DatasetManagementException {
    return false;
  }

  @Override
  public SettableArguments getArguments() {
    return new SettableArguments() {
      @Override
      public boolean has(String name) {
        return settableArguments.containsKey(name);
      }

      @Nullable
      @Override
      public String get(String name) {
        return settableArguments.get(name);
      }

      @Override
      public void set(String name, String value) {
        settableArguments.put(name, value);
      }

      @Override
      public Map<String, String> asMap() {
        return settableArguments;
      }

      @Override
      public Iterator<Map.Entry<String, String>> iterator() {
        return settableArguments.entrySet().iterator();
      }
    };
  }

  @Override
  public FailureCollector getFailureCollector() {
    return this.failureCollector;
  }

  @Nullable
  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return null;
  }

  @Nullable
  @Override
  public URL getServiceURL(String serviceId) {
    return null;
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity)
      throws MetadataException {
    return null;
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity)
      throws MetadataException {
    return null;
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {}

  @Override
  public void addTags(MetadataEntity metadataEntity, String... tags) {}

  @Override
  public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {}

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {}

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {}

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... keys) {}

  @Override
  public void removeTags(MetadataEntity metadataEntity) {}

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... tags) {}

  @Override
  public void record(List<FieldOperation> fieldOperations) {}

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return null;
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name)
      throws DatasetInstantiationException {
    return null;
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
    return null;
  }

  @Override
  public <T extends Dataset> T getDataset(
      String namespace, String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
    return null;
  }

  @Override
  public void releaseDataset(Dataset dataset) {}

  @Override
  public void discardDataset(Dataset dataset) {}

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return null;
  }
}
