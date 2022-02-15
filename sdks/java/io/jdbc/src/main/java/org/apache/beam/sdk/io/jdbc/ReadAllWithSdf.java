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
package org.apache.beam.sdk.io.jdbc;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceProviderFromDataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.io.jdbc.JdbcUtil.JdbcReadWithPartitionsHelper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadAllWithSdf<ParameterT, OutputT>
    extends PTransform<PCollection<KV<ParameterT, ParameterT>>, PCollection<OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadAllWithSdf.class);

  abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

  abstract @Nullable ValueProvider<String> getQuery();

  abstract JdbcIO.PreparedStatementSetter<KV<ParameterT, ParameterT>> getParameterSetter();

  abstract JdbcIO.RowMapper<OutputT> getRowMapper();

  abstract @Nullable JdbcReadWithPartitionsHelper<ParameterT> getHelper();

  abstract @Nullable Coder<KV<ParameterT, ParameterT>> getRestrictionCoder();

  abstract @Nullable Coder<OutputT> getCoder();

  abstract int getFetchSize();

  abstract String getPartitioningColumn();

  abstract ReadAllWithSdf.Builder<ParameterT, OutputT> toBuilder();

  public static <ParameterT, OutputT> ReadAllWithSdf<ParameterT, OutputT> create() {
    return new AutoValue_ReadAllWithSdf.Builder<ParameterT, OutputT>()
        .setPartitioningColumn("")
        .setFetchSize(1000)
        .setRowMapper(
            new RowMapper<OutputT>() {
              @Override
              public OutputT mapRow(ResultSet resultSet) throws Exception {
                return null;
              }
            })
        .setParameterSetter(
            new PreparedStatementSetter<KV<ParameterT, ParameterT>>() {
              @Override
              public void setParameters(
                  KV<ParameterT, ParameterT> element, PreparedStatement preparedStatement)
                  throws Exception {
                return;
              }
            })
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder<ParameterT, OutputT> {
    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setParameterSetter(
        PreparedStatementSetter<KV<ParameterT, ParameterT>> parameterSetter);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setHelper(
        JdbcReadWithPartitionsHelper<ParameterT> helper);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setFetchSize(int fetchSize);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setPartitioningColumn(
        String partitioningColumn);

    abstract ReadAllWithSdf.Builder<ParameterT, OutputT> setRestrictionCoder(
        Coder<KV<ParameterT, ParameterT>> restrictionCoder);

    abstract ReadAllWithSdf<ParameterT, OutputT> build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withDataSourceConfiguration(
      DataSourceConfiguration config) {
    return withDataSourceProviderFn(DataSourceProviderFromDataSourceConfiguration.of(config));
  }

  public ReadAllWithSdf<ParameterT, OutputT> withDataSourceProviderFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn) {
    if (getDataSourceProviderFn() != null) {
      throw new IllegalArgumentException(
          "A dataSourceConfiguration or dataSourceProviderFn has "
              + "already been provided, and does not need to be provided again.");
    }
    return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withRestrictionCoder(
      Coder<KV<ParameterT, ParameterT>> restrictionCoder) {
    return toBuilder().setRestrictionCoder(restrictionCoder).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withQuery(String query) {
    checkArgument(query != null, "ReadAllWithSdf().withQuery(query) called with null query");
    return withQuery(ValueProvider.StaticValueProvider.of(query));
  }

  public ReadAllWithSdf<ParameterT, OutputT> withQuery(ValueProvider<String> query) {
    checkArgument(query != null, "ReadAllWithSdf().withQuery(query) called with null query");
    return toBuilder().setQuery(query).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withParameterSetter(
      PreparedStatementSetter<KV<ParameterT, ParameterT>> parameterSetter) {
    checkArgument(
        parameterSetter != null,
        "ReadAllWithSdf().withParameterSetter(parameterSetter) called "
            + "with null statementPreparator");
    return toBuilder().setParameterSetter(parameterSetter).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
    checkArgument(
        rowMapper != null, "ReadAllWithSdf().withRowMapper(rowMapper) called with null rowMapper");
    return toBuilder().setRowMapper(rowMapper).build();
  }

  /**
   * @deprecated
   *     <p>{@link JdbcIO} is able to infer aprppriate coders from other parameters.
   */
  @Deprecated
  public ReadAllWithSdf<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
    checkArgument(coder != null, "ReadAllWithSdf().withCoder(coder) called with null coder");
    return toBuilder().setCoder(coder).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withHelper(
      JdbcReadWithPartitionsHelper<ParameterT> helper) {
    return toBuilder().setHelper(helper).build();
  }

  public ReadAllWithSdf<ParameterT, OutputT> withPartitioningColumn(String column) {
    return toBuilder().setPartitioningColumn(column).build();
  }

  /**
   * This method is used to set the size of the data that is going to be fetched and loaded in
   * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
   * It should ONLY be used if the default value throws memory errors.
   */
  public ReadAllWithSdf<ParameterT, OutputT> withFetchSize(int fetchSize) {
    checkArgument(fetchSize > 0, "fetch size must be >0");
    return toBuilder().setFetchSize(fetchSize).build();
  }

  private Coder<OutputT> inferCoder(CoderRegistry registry, SchemaRegistry schemaRegistry) {
    if (getCoder() != null) {
      return getCoder();
    } else {
      RowMapper<OutputT> rowMapper = getRowMapper();
      TypeDescriptor<OutputT> outputType =
          TypeDescriptors.extractFromTypeParameters(
              rowMapper,
              RowMapper.class,
              new TypeVariableExtractor<RowMapper<OutputT>, OutputT>() {});
      try {
        return schemaRegistry.getSchemaCoder(outputType);
      } catch (NoSuchSchemaException e) {
        LOG.warn(
            "Unable to infer a schema for type {}. Attempting to infer a coder without a schema.",
            outputType);
      }
      try {
        return registry.getCoder(outputType);
      } catch (CannotProvideCoderException e) {
        LOG.warn("Unable to infer a coder for type {}", outputType);
        return null;
      }
    }
  }

  @Override
  public PCollection<OutputT> expand(PCollection<KV<ParameterT, ParameterT>> input) {
    Coder<OutputT> coder =
        inferCoder(input.getPipeline().getCoderRegistry(), input.getPipeline().getSchemaRegistry());
    checkNotNull(
        coder,
        "Unable to infer a coder for ReadAllWithSdf() transform. "
            + "Provide a coder via withCoder, or ensure that one can be inferred from the"
            + " provided RowMapper.");
    PCollection<OutputT> output =
        input
            .apply(
                ParDo.of(
                    new ReadSDF<ParameterT, OutputT>(
                        getDataSourceProviderFn(),
                        getQuery().get(),
                        getParameterSetter(),
                        getRowMapper(),
                        getFetchSize(),
                        getHelper(),
                        rs -> {
                          try {
                            return (ParameterT) rs.getObject(getPartitioningColumn());
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }
                        },
                        getRestrictionCoder())))
            .setCoder(coder);

    try {
      TypeDescriptor<OutputT> typeDesc = coder.getEncodedTypeDescriptor();
      SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
      Schema schema = registry.getSchema(typeDesc);
      output.setSchema(
          schema,
          typeDesc,
          registry.getToRowFunction(typeDesc),
          registry.getFromRowFunction(typeDesc));
    } catch (NoSuchSchemaException e) {
      // ignore
    }

    return output;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", getQuery()));
    builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
    if (getCoder() != null) {
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
    }
    if (getDataSourceProviderFn() instanceof HasDisplayData) {
      ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
    }
  }
}
