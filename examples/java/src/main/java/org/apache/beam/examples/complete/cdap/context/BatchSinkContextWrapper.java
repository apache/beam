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
package org.apache.beam.examples.complete.cdap.context;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Output;
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
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.List;
import java.util.Map;

@SuppressWarnings("TypeParameterUnusedInFormals")
public class BatchSinkContextWrapper implements BatchSinkContext {

    private OperationContext context;

    BatchSinkContextWrapper() {
       this.context = new OperationContext();
    }

    @Override
    public FailureCollector getFailureCollector() {
        return this.context.getFailureCollector();
    }

    @Override
    public void addOutput(Output output) {

    }

    @Override
    public boolean isPreviewEnabled() {
        return false;
    }

    @Override
    public void createDataset(String datasetName, String typeName, DatasetProperties properties) throws DatasetManagementException {

    }

    @Override
    public boolean datasetExists(String datasetName) throws DatasetManagementException {
        return false;
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
        return this.context.getLogicalStartTime();
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

    @Nullable
    @Override
    public Schema getInputSchema() {
        return null;
    }

    @Override
    public Map<String, Schema> getInputSchemas() {
        return null;
    }

    @Nullable
    @Override
    public Schema getOutputSchema() {
        return null;
    }

    @Override
    public Map<String, Schema> getOutputPortSchemas() {
        return null;
    }

    @Override
    public SettableArguments getArguments() {
        return null;
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
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
        return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
        return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) throws DatasetInstantiationException {
        return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments) throws DatasetInstantiationException {
        return null;
    }

    @Override
    public void releaseDataset(Dataset dataset) {

    }

    @Override
    public void discardDataset(Dataset dataset) {

    }

    @Override
    public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
        return null;
    }

    @Override
    public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
        return null;
    }

    @Override
    public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
        return null;
    }

    @Override
    public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {

    }

    @Override
    public void removeMetadata(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeProperties(MetadataEntity metadataEntity, String... keys) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity) {

    }

    @Override
    public void removeTags(MetadataEntity metadataEntity, String... tags) {

    }

    @Override
    public void record(List<FieldOperation> fieldOperations) {

    }
}
