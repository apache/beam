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
package org.apache.beam.sdk.io.datalake;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.*;

public class DataLakeIO {

    private static final Logger LOG = LoggerFactory.getLogger(DataLakeIO.class);

    /**
     * Read data from a data lake datasource.
     *
     * @param <T> Type of the data to be read.
     */
    public static <T> DataLakeIO.Read<T> read() {
        return new AutoValue_DataLakeIO_Read.Builder<T>()
                .setFetchSize(DEFAULT_FETCH_SIZE)
                .build();
    }

    /**
     * Like {@link #read}, but executes multiple instances of the query substituting each element of a
     * {@link PCollection} as query parameters.
     *
     * @param <ParameterT> Type of the data representing query parameters.
     * @param <OutputT> Type of the data to be read.
     */
    public static <ParameterT, OutputT> DataLakeIO.ReadAll<ParameterT, OutputT> readAll() {
        return new AutoValue_DataLakeIO_ReadAll.Builder<ParameterT, OutputT>()
                .setFetchSize(DEFAULT_FETCH_SIZE)
                .build();
    }


    /** Write data to data lake. */
    public static Write write() {
        return new AutoValue_DataLakeIO_Write.Builder()
                .build();
    }

    private static final int DEFAULT_FETCH_SIZE = 50_000;

    private DataLakeIO() {}

    /**
     * An interface used by DataLakeIO. Read for converting each row of the RowRecord into
     * an element of the resulting {@link PCollection}.
     */
    @FunctionalInterface
    public interface RowMapper<T> extends Serializable {
        T mapRow(Row rowRecord) throws Exception;
    }

    /** Implementation of {@link #read}. */
    @AutoValue
    public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

        abstract @Nullable ValueProvider<String> getQuery();

        abstract @Nullable String getPath();

        abstract @Nullable String getFormat();

        abstract @Nullable SparkConf getSparkConf();

        abstract @Nullable RowMapper<T> getRowMapper();

        abstract @Nullable Coder<T> getCoder();

        abstract int getFetchSize();

        abstract DataLakeIO.Read.Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {

            abstract DataLakeIO.Read.Builder<T> setQuery(ValueProvider<String> query);

            abstract DataLakeIO.Read.Builder<T> setPath(String path);

            abstract DataLakeIO.Read.Builder<T> setFormat(String format);

            abstract DataLakeIO.Read.Builder<T> setSparkConf(SparkConf sparkConf);

            abstract DataLakeIO.Read.Builder<T> setRowMapper(DataLakeIO.RowMapper<T> rowMapper);

            abstract DataLakeIO.Read.Builder<T> setCoder(Coder<T> coder);

            abstract DataLakeIO.Read.Builder<T> setFetchSize(int fetchSize);

            abstract DataLakeIO.Read<T> build();
        }

        public DataLakeIO.Read<T> withQuery(String query) {
            return withQuery(ValueProvider.StaticValueProvider.of(query));
        }

        public DataLakeIO.Read<T> withQuery(ValueProvider<String> query) {
            return toBuilder().setQuery(query).build();
        }

        public DataLakeIO.Read<T> withPath(String path){
            return toBuilder().setPath(path).build();
        }

        public DataLakeIO.Read<T> withFormat(String format){
            checkArgument(format != null, "format can not be null");
            return toBuilder().setFormat(format).build();
        }

        public DataLakeIO.Read<T> withSparkConf(SparkConf sparkConf){
            checkArgument(sparkConf != null, "sparkConf can not be null");
            return toBuilder().setSparkConf(sparkConf).build();
        }

        public DataLakeIO.Read<T> withRowMapper(DataLakeIO.RowMapper<T> rowMapper) {
            checkArgument(rowMapper != null, "rowMapper can not be null");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public DataLakeIO.Read<T> withCoder(Coder<T> coder) {
            checkArgument(coder != null, "coder can not be null");
            return toBuilder().setCoder(coder).build();
        }

        /**
         * This method is used to set the size of the data that is going to be fetched and loaded in
         * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
         * It should ONLY be used if the default value throws memory errors.
         */
        public DataLakeIO.Read<T> withFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "fetch size must be > 0");
            return toBuilder().setFetchSize(fetchSize).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            checkArgument(getRowMapper() != null, "withRowMapper() is required");

            DataLakeIO.ReadAll<Void, T> readAll =
                    DataLakeIO.<Void, T>readAll()
                            .withQuery(getQuery())
                            .withPath(getPath())
                            .withFormat(getFormat())
                            .withSparkConf(getSparkConf())
                            .withRowMapper(getRowMapper())
                            .withFetchSize(getFetchSize())
                    ;

            if (getCoder() != null) {
                readAll = readAll.toBuilder().setCoder(getCoder()).build();
            }
            return input.apply(Create.of((Void) null)).apply(readAll);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            if(getQuery() != null){
                builder.add(DisplayData.item("query", getQuery()));
            }
            builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
            if (getCoder() != null) {
                builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
            }
        }
    }

    /** Implementation of {@link #readAll}. */
    @AutoValue
    public abstract static class ReadAll<ParameterT, OutputT>
            extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

        abstract @Nullable ValueProvider<String> getQuery();

        abstract @Nullable String getPath();

        abstract @Nullable String getFormat();

        abstract @Nullable SparkConf getSparkConf();

        abstract @Nullable RowMapper<OutputT> getRowMapper();

        abstract @Nullable Coder<OutputT> getCoder();

        abstract int getFetchSize();

        abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<ParameterT, OutputT> {

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setPath(String path);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setFormat(String format);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setSparkConf(SparkConf sparkConf);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setRowMapper(DataLakeIO.RowMapper<OutputT> rowMapper);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

            abstract DataLakeIO.ReadAll.Builder<ParameterT, OutputT> setFetchSize(int fetchSize);

            abstract DataLakeIO.ReadAll<ParameterT, OutputT> build();
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withQuery(String query) {
            return withQuery(ValueProvider.StaticValueProvider.of(query));
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withQuery(ValueProvider<String> query) {
            return toBuilder().setQuery(query).build();
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withPath(String path){
            return toBuilder().setPath(path).build();
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withFormat(String format){
            return toBuilder().setFormat(format).build();
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withSparkConf(SparkConf sparkConf){
            return toBuilder().setSparkConf(sparkConf).build();
        }

        public DataLakeIO.ReadAll<ParameterT, OutputT> withRowMapper(DataLakeIO.RowMapper<OutputT> rowMapper) {
            checkArgument(
                    rowMapper != null,
                    "DataLakeIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        /**
         * @deprecated
         *     <p>{@link DataLakeIO} is able to infer aprppriate coders from other parameters.
         */
        @Deprecated
        public DataLakeIO.ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
            checkArgument(coder != null, "DataLakeIO.readAll().withCoder(coder) called with null coder");
            return toBuilder().setCoder(coder).build();
        }

        /**
         * This method is used to set the size of the data that is going to be fetched and loaded in
         * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
         * It should ONLY be used if the default value throws memory errors.
         */
        public DataLakeIO.ReadAll<ParameterT, OutputT> withFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "fetch size must be >0");
            return toBuilder().setFetchSize(fetchSize).build();
        }

        private Coder<OutputT> inferCoder(CoderRegistry registry, SchemaRegistry schemaRegistry) {
            if (getCoder() != null) {
                return getCoder();
            } else {
                DataLakeIO.RowMapper<OutputT> rowMapper = getRowMapper();
                TypeDescriptor<OutputT> outputType =
                        TypeDescriptors.extractFromTypeParameters(
                                rowMapper,
                                DataLakeIO.RowMapper.class,
                                new TypeVariableExtractor<DataLakeIO.RowMapper<OutputT>, OutputT>() {});
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
        public PCollection<OutputT> expand(PCollection<ParameterT> input) {
            Coder<OutputT> coder =
                    inferCoder(
                            input.getPipeline().getCoderRegistry(), input.getPipeline().getSchemaRegistry());
            checkNotNull(
                    coder,
                    "Unable to infer a coder for DataLakeIO.readAll() transform. "
                            + "Provide a coder via withCoder, or ensure that one can be inferred from the"
                            + " provided RowMapper.");
            PCollection<OutputT> output =
                    input
                            .apply(
                                    ParDo.of(
                                            new DataLakeIO.ReadFn<>(
                                                    getPath(),
                                                    getFormat(),
                                                    getSparkConf(),
                                                    getRowMapper()
                                            )))
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
            if(getQuery() != null){
                builder.add(DisplayData.item("query", getQuery()));
            }
            builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
            if (getCoder() != null) {
                builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
            }
        }
    }

    private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {

        private final String path;
        private final String format;
        private final SparkConf sparkConf;
        private final DataLakeIO.RowMapper<OutputT> rowMapper;

        private ReadFn(
                String path,
                String format,
                SparkConf sparkConf,
                DataLakeIO.RowMapper<OutputT> rowMapper
        ) {
            this.path = path;
            this.format = format;
            this.sparkConf = sparkConf;
            this.rowMapper = rowMapper;
        }

        @ProcessElement
        // Spotbugs seems to not understand the nested try-with-resources
//        @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
        public void processElement(ProcessContext context) throws Exception {
            SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            Dataset<Row> dataset = sparkSession.read().format(format).load(path);
            Iterator<Row> iterator = dataset.toLocalIterator();
            while(iterator.hasNext()){
                Row row = iterator.next();
                context.output(rowMapper.mapRow(row));
            }
            sparkSession.close();
        }
    }

    @AutoValue
    public abstract static class Write extends PTransform<PCollection<Row>, PDone> {

        abstract @Nullable String getPath();

        abstract @Nullable SparkConf getSparkConf();

        abstract @Nullable String getFormat();

        abstract @Nullable String getMode();

        abstract @Nullable StructType getSchema();

        abstract @Nullable Map<String, String> getOptions();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {

            abstract Builder setPath(String path);

            abstract Builder setSparkConf(SparkConf sparkConf);

            abstract Builder setFormat(String format);

            abstract Builder setMode(String mode);

            abstract Builder setSchema(StructType schema);

            abstract Builder setOptions(Map<String, String> options);

            abstract Write build();
        }

        public Write withPath(String path){
            checkArgument(path != null, "path can not be null");
            return toBuilder().setPath(path).build();
        }

        public Write withSparkConf(SparkConf sparkConf){
            checkArgument(sparkConf != null, "sparkConf can not be null");
            return toBuilder().setSparkConf(sparkConf).build();
        }

        public Write withFormat(String format){
            checkArgument(format != null, "format can not be null");
            return toBuilder().setFormat(format).build();
        }

        public Write withMode(String mode){
            checkArgument(mode != null, "mode can not be null");
            return toBuilder().setMode(mode).build();
        }

        public Write withSchema(StructType schema){
            checkArgument(schema != null, "schema can not be null");
            return toBuilder().setSchema(schema).build();
        }

        public Write withOptions(Map<String, String> options){
            return toBuilder().setOptions(options).build();
        }

        @Override
        public PDone expand(PCollection<Row> input) {
            input.apply(ParDo.of(new WriteFn(this)));
            return PDone.in(input.getPipeline());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
        }

        private static class WriteFn extends DoFn<Row, Void> {

            private final Write spec;

            public WriteFn(Write spec) {
                this.spec = spec;
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {

                Row row = c.element();
                write2DataLake(row);
            }

            private void write2DataLake(Row row) throws Exception {

                SparkConf sparkConf = spec.getSparkConf();

                SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

                StructType schema = spec.getSchema();

                List<Row> rows = new ArrayList<>();
                rows.add(row);

                Dataset<Row> df = sparkSession.createDataFrame(rows, schema);

                if("hudi".equals(spec.getFormat())) {
                    df.write().format(spec.getFormat()).mode(spec.getMode()).options(spec.getOptions()).save(spec.getPath());
                }else if("iceberg".equals(spec.getFormat())){
                    if("append".equals(spec.getMode())){
                        df.writeTo(spec.getPath()).append();
                    }else if("overwrite".equals(spec.getMode())){
                        df.writeTo(spec.getPath()).overwritePartitions();
                    }else{
                        throw new Exception("unsupported mode:："+ spec.getMode());
                    }
                }else{
                    df.write().format(spec.getFormat()).mode(spec.getMode()).save(spec.getPath());
                }
            }

        }
    }

}

