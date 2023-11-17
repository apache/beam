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
package org.apache.beam.io.iceberg;

import static org.apache.beam.sdk.values.TypeDescriptors.extractFromTypeParameters;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
public abstract class DynamicDestinations<T, DestinationT> implements Serializable {

  interface SideInputAccessor {
    <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view);
  }

  private transient SideInputAccessor sideInputAccessor;
  private transient PipelineOptions options;

  static class ProcessContextSideInputAccessor implements SideInputAccessor {
    private DoFn<?, ?>.ProcessContext processContext;

    public ProcessContextSideInputAccessor(DoFn<?, ?>.ProcessContext processContext) {
      this.processContext = processContext;
    }

    @Override
    public <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
      return processContext.sideInput(view);
    }
  }

  public PipelineOptions getOptions() {
    return options;
  }

  public List<PCollectionView<?>> getSideInputs() {
    return Lists.newArrayList();
  }

  protected final <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
    checkState(
        getSideInputs().contains(view),
        "View %s not declared in getSideInputs() (%s)",
        view,
        getSideInputs());
    if (sideInputAccessor == null) {
      throw new IllegalStateException("sideInputAccessor (transient field) is null");
    }
    return sideInputAccessor.sideInput(view);
  }

  void setSideInputProcessContext(DoFn<?, ?>.ProcessContext context) {
    this.sideInputAccessor = new ProcessContextSideInputAccessor(context);
    this.options = context.getPipelineOptions();
  }

  public abstract DestinationT getDestination(ValueInSingleWindow<T> element);

  public Coder<DestinationT> getDestinationCoder() {
    return null;
  }

  public abstract Table getTable(DestinationT destination);

  public abstract Schema getSchema(DestinationT destination);

  public abstract PartitionSpec getPartitionSpec(DestinationT destination);

  public abstract FileFormat getFileFormat(DestinationT destination);

  Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
      throws CannotProvideCoderException {
    Coder<DestinationT> destinationCoder = getDestinationCoder();
    if (destinationCoder != null) {
      return destinationCoder;
    }
    TypeDescriptor<DestinationT> descriptor =
        extractFromTypeParameters(
            this,
            DynamicDestinations.class,
            new TypeDescriptors.TypeVariableExtractor<
                DynamicDestinations<T, DestinationT>, DestinationT>() {});
    try {
      return registry.getCoder(descriptor);
    } catch (CannotProvideCoderException e) {
      throw new CannotProvideCoderException(
          "Failed to infer coder for DestinationT from type "
              + descriptor
              + ", please provide it explicitly by overriding getDestinationCoder()",
          e);
    }
  }

  public static class StaticTableDestination<ElementT>
      extends DynamicDestinations<ElementT, String> {

    final Iceberg.Table table;

    public StaticTableDestination(Iceberg.Table table) {
      this.table = table;
    }

    @Override
    public String getDestination(ValueInSingleWindow<ElementT> element) {
      return table.table().name();
    }

    @Override
    public Table getTable(String destination) {
      return table.table();
    }

    @Override
    public Schema getSchema(String destination) {
      return getTable(destination).schema();
    }

    @Override
    public PartitionSpec getPartitionSpec(String destination) {
      return getTable(destination).spec();
    }

    @Override
    public FileFormat getFileFormat(String destination) {
      return FileFormat.PARQUET;
    }
  }

  public static <ElementT> StaticTableDestination<ElementT> constant(Iceberg.Table table) {
    return new StaticTableDestination<>(table);
  }
}
