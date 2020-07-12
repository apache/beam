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
package org.apache.beam.sdk.io.influxdb;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.influxdb.BatchOptions.DEFAULT_BATCH_INTERVAL_DURATION;
import static org.influxdb.BatchOptions.DEFAULT_BUFFER_LIMIT;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write to InfluxDB.
 *
 * <h3>Reading from InfluxDB</h3>
 *
 * <p>InfluxDBIO {@link #read()} returns a bounded collection of {@code String} as a {@code
 * PCollection<String>}.
 *
 * <p>You have to provide a {@link DataSourceConfiguration} using<br>
 * {@link DataSourceConfiguration#create(String, String, String)}(url, userName and password).
 * Optionally, {@link DataSourceConfiguration#withUsername(String)} and {@link
 * DataSourceConfiguration#withPassword(String)} allows you to define userName and password.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<String> collection = pipeline.apply(InfluxDBIO.read()
 *   .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(
 *          "https://localhost:8086","userName","password"))
 *   .withDatabase("metrics")
 *   .withRetentionPolicy("autogen")
 *   .withSslInvalidHostNameAllowed(true)
 *   .withSslEnabled(true));
 * }</pre>
 *
 * <p>Read with query example:
 *
 * <pre>{@code
 * PCollection<String> collection = pipeline.apply(InfluxDBIO.read()
 *   .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(
 *          "https://localhost:8086","userName","password"))
 *   .withDatabase("metrics")
 *   .withQuery("SELECT * FROM CPU")
 *   .withRetentionPolicy("autogen")
 *   .withSslInvalidHostNameAllowed(true)
 *   .withSslEnabled(true));
 * }</pre>
 *
 * <h3>Writing to InfluxDB </h3>
 *
 * <p>InfluxDB sink supports writing records into a database. It writes a {@link PCollection} to the
 * database. InfluxDB line protocol reference
 *
 * <p>Like the {@link #read()}, to configure the {@link #write()}, you have to provide a {@link
 * DataSourceConfiguration}.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(InfluxDb.write()
 *      .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(
 *            "https://localhost:8086","userName","password"))
 *   .withRetentionPolicy("autogen")
 *   .withDatabase("metrics")
 *   .withSslInvalidHostNameAllowed(true)
 *   .withSslEnabled(true));
 *    );
 * }</pre>
 *
 * *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class InfluxDbIO {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDbIO.class);

  public static Write write() {
    return new AutoValue_InfluxDbIO_Write.Builder()
        .setSslEnabled(false)
        .setSslInvalidHostNameAllowed(false)
        .setFlushDuration(DEFAULT_BATCH_INTERVAL_DURATION)
        .setNumOfElementsToBatch(DEFAULT_BUFFER_LIMIT)
        .build();
  }

  public static Read read() {
    return new AutoValue_InfluxDbIO_Read.Builder()
        .setSslEnabled(false)
        .setSslInvalidHostNameAllowed(false)
        .setStartDateTime(DateTime.parse("1677-09-21T00:12:43.145224194Z"))
        .setEndDateTime(DateTime.parse("2262-04-11T23:47:16.854775806Z"))
        .setRetentionPolicy("autogen")
        .build();
  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    abstract boolean sslInvalidHostNameAllowed();

    abstract String retentionPolicy();

    @Nullable
    abstract String database();

    @Nullable
    abstract String query();

    abstract boolean sslEnabled();

    @Nullable
    abstract DataSourceConfiguration dataSourceConfiguration();

    @Nullable
    abstract List<String> metrics();

    abstract DateTime startDateTime();

    abstract DateTime endDateTime();

    @Nullable
    abstract DateTime fromDateTime();

    @Nullable
    abstract DateTime toDateTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);

      abstract Builder setDatabase(String database);

      abstract Builder setSslInvalidHostNameAllowed(boolean value);

      abstract Builder setRetentionPolicy(String retentionPolicy);

      abstract Builder setQuery(String query);

      abstract Builder setToDateTime(DateTime toDateTime);

      abstract Builder setFromDateTime(DateTime fromDateTime);

      abstract Builder setStartDateTime(DateTime startDateTime);

      abstract Builder setEndDateTime(DateTime endDateTime);

      abstract Builder setSslEnabled(boolean sslEnabled);

      abstract Builder setMetrics(List<String> metrics);

      abstract Read build();
    }

    /** Reads from the InfluxDB instance indicated by the given configuration. */
    public Read withDataSourceConfiguration(DataSourceConfiguration configuration) {
      checkState(configuration != null, "configuration can not be null");
      return builder().setDataSourceConfiguration(configuration).build();
    }

    /** Reads from the specified database. */
    public Read withDatabase(String database) {
      return builder()
          .setDatabase(database)
          .setDataSourceConfiguration(dataSourceConfiguration())
          .build();
    }

    public Read withToDateTime(DateTime toDateTime) {
      return builder().setToDateTime(toDateTime).build();
    }

    public Read withFromDateTime(DateTime fromDateTime) {
      return builder().setFromDateTime(fromDateTime).build();
    }

    public Read withStartDateTime(DateTime startDateTime) {
      return builder().setStartDateTime(startDateTime).build();
    }

    public Read withEndDateTime(DateTime endDateTime) {
      return builder().setEndDateTime(endDateTime).build();
    }

    public Read withMetrics(List<String> metrics) {
      return builder().setMetrics(metrics).build();
    }

    public Read withMetrics(String... metrics) {
      return withMetrics(Arrays.asList(metrics));
    }

    public Read withSslEnabled(boolean sslEnabled) {
      return builder().setSslEnabled(sslEnabled).build();
    }

    public Read withSslInvalidHostNameAllowed(boolean value) {
      return builder().setSslInvalidHostNameAllowed(value).build();
    }

    public Read withRetentionPolicy(String retentionPolicy) {
      return builder().setRetentionPolicy(retentionPolicy).build();
    }

    public Read withQuery(String query) {
      return builder().setQuery(query).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkState(dataSourceConfiguration() != null, "configuration is required");
      checkState(query() != null || database() != null, "database or query is required");
      if (database() != null) {
        checkState(
            checkDatabase(
                database(), dataSourceConfiguration(), sslInvalidHostNameAllowed(), sslEnabled()),
            "Database %s does not exist",
            database());
      }
      return input.apply(org.apache.beam.sdk.io.Read.from(new InfluxDBSource(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("dataSourceConfiguration", dataSourceConfiguration().toString()));
      builder.addIfNotNull(DisplayData.item("database", database()));
      builder.addIfNotNull(DisplayData.item("retentionPolicy", retentionPolicy()));
      builder.addIfNotNull(DisplayData.item("sslEnabled", sslEnabled()));
      builder.addIfNotNull(DisplayData.item("query", query()));
      builder.addIfNotNull(
          DisplayData.item("sslInvalidHostNameAllowed", sslInvalidHostNameAllowed()));
    }
  }

  static class InfluxDBSource extends BoundedSource<String> {
    private final Read spec;

    InfluxDBSource(Read read) {
      this.spec = read;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      String numOfBlocks = "NUMBER OF BLOCKS";
      String sizeOfBlocks = "SIZE OF BLOCKS";
      LinkedHashSet<Long> numOfBlocksValue = new LinkedHashSet<>();
      LinkedHashSet<Long> sizeOfBlocksValue = new LinkedHashSet<>();
      try (InfluxDB connection =
          getConnection(
              spec.dataSourceConfiguration(),
              spec.sslInvalidHostNameAllowed(),
              spec.sslEnabled())) {
        String query = spec.query();
        if (query == null) {
          query =
              String.format(
                  "SELECT * FROM %s.%s", spec.retentionPolicy(), String.join(",", spec.metrics()));
        }
        QueryResult result = connection.query(new Query(query, spec.database()));
        List<Result> results = result.getResults();
        for (Result res : results) {
          for (Series series : res.getSeries()) {
            for (List<Object> data : series.getValues()) {
              String s = data.get(0).toString();
              if (s.startsWith(numOfBlocks)) {
                numOfBlocksValue.add(Long.parseLong(s.split(":", -1)[1].trim()));
              }
              if (s.startsWith(sizeOfBlocks)) {
                sizeOfBlocksValue.add(Long.parseLong(s.split(":", -1)[1].trim()));
              }
            }
          }
        }
      }

      Iterator<Long> numOfBlocksValueIterator = numOfBlocksValue.iterator();
      Iterator<Long> sizeOfBlocksValueIterator = sizeOfBlocksValue.iterator();
      long size = 0;
      while (numOfBlocksValueIterator.hasNext() && sizeOfBlocksValueIterator.hasNext()) {
        size = size + (numOfBlocksValueIterator.next() * sizeOfBlocksValueIterator.next());
      }
      return size;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredElementsInABundle, PipelineOptions options) {
      List<ShardInformation> shardInfo =
          getDBShardedInformation(
              spec.database(),
              spec.dataSourceConfiguration(),
              spec.sslInvalidHostNameAllowed(),
              spec.sslEnabled());
      List<BoundedSource<String>> sources = new ArrayList<>();
      if (spec.query() == null) {
        for (ShardInformation sInfo : shardInfo) {
          if (sInfo.getStartTime().compareTo(spec.startDateTime()) > 0) {
            sources.add(
                new InfluxDBSource(
                    spec.withMetrics(spec.metrics())
                        .withRetentionPolicy(sInfo.getRetentionPolicy())
                        .withToDateTime(sInfo.getStartTime())
                        .withFromDateTime(sInfo.getEndTime())));
          }
        }
      } else {
        sources.add(this);
      }
      return sources;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions pipelineOptions) {
      return new BoundedInfluxDbReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null /* input */);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static String getQueryToRun(Read spec) {
    if (spec.query() == null) {
      if (spec.toDateTime() != null && spec.fromDateTime() != null) {
        String retVal =
            String.format(
                "SELECT * FROM %s.%s WHERE time >= '%s' and time <= '%s'",
                spec.retentionPolicy(),
                String.join(",", spec.metrics()),
                spec.toDateTime(),
                spec.fromDateTime());
        return retVal;
      } else {
        return String.format(
            "SELECT * FROM %s.%s", spec.retentionPolicy(), String.join(",", spec.metrics()));
      }
    }
    return spec.query();
  }

  private static class BoundedInfluxDbReader extends BoundedSource.BoundedReader<String> {
    private final InfluxDbIO.InfluxDBSource source;
    private Iterator<Result> resultIterator;
    private Iterator<Series> seriesIterator;
    private Iterator<List<Object>> valuesIterator;
    private List current;

    public BoundedInfluxDbReader(InfluxDbIO.InfluxDBSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      InfluxDbIO.Read spec = source.spec;
      try (InfluxDB influxDB =
          getConnection(
              spec.dataSourceConfiguration(),
              spec.sslInvalidHostNameAllowed(),
              spec.sslEnabled())) {
        if (spec.database() != null) {
          influxDB.setDatabase(spec.database());
        }
        if (spec.retentionPolicy() != null) {
          influxDB.setRetentionPolicy(spec.retentionPolicy());
        }
        String query = getQueryToRun(spec);
        QueryResult queryResult = influxDB.query(new Query(query, spec.database()));
        resultIterator = queryResult.getResults().iterator();
        if (resultIterator.hasNext()) {
          seriesIterator = resultIterator.next().getSeries().iterator();
        }
        if (seriesIterator.hasNext()) {
          valuesIterator = seriesIterator.next().getValues().iterator();
        }
      }
      return advance();
    }

    @Override
    public boolean advance() {
      if (valuesIterator.hasNext()) {
        current = valuesIterator.next();
        return true;
      } else if (seriesIterator.hasNext()) {
        valuesIterator = seriesIterator.next().getValues().iterator();
        current = valuesIterator.next();
        return true;
      } else if (resultIterator.hasNext()) {
        seriesIterator = resultIterator.next().getSeries().iterator();
        valuesIterator = seriesIterator.next().getValues().iterator();
        current = valuesIterator.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public BoundedSource getCurrentSource() {
      return source;
    }

    @Override
    public String getCurrent() {
      return current.toString();
    }

    @Override
    public void close() {}
  }

  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Override
    public PDone expand(PCollection<String> input) {
      checkState(dataSourceConfiguration() != null, "withConfiguration() is required");
      checkState(database() != null && !database().isEmpty(), "withDatabase() is required");
      checkState(
          checkDatabase(
              database(), dataSourceConfiguration(), sslInvalidHostNameAllowed(), sslEnabled()),
          "Database %s does not exist",
          database());
      input.apply(ParDo.of(new InfluxWriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("dataSourceConfiguration", dataSourceConfiguration().toString()));
      builder.addIfNotNull(DisplayData.item("database", database()));
      builder.addIfNotNull(DisplayData.item("retentionPolicy", retentionPolicy()));
      builder.addIfNotNull(DisplayData.item("sslEnabled", sslEnabled()));
      builder.addIfNotNull(
          DisplayData.item("sslInvalidHostNameAllowed", sslInvalidHostNameAllowed()));
      builder.addIfNotNull(DisplayData.item("numOfElementsToBatch", numOfElementsToBatch()));
      builder.addIfNotNull(DisplayData.item("flushDuration", flushDuration()));
    }

    @Nullable
    abstract String database();

    @Nullable
    abstract String retentionPolicy();

    abstract boolean sslInvalidHostNameAllowed();

    abstract boolean sslEnabled();

    abstract int numOfElementsToBatch();

    abstract int flushDuration();

    @Nullable
    abstract DataSourceConfiguration dataSourceConfiguration();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);

      abstract Builder setDatabase(String database);

      abstract Builder setSslInvalidHostNameAllowed(boolean value);

      abstract Builder setNumOfElementsToBatch(int numOfElementsToBatch);

      abstract Builder setFlushDuration(int flushDuration);

      abstract Builder setSslEnabled(boolean sslEnabled);

      abstract Builder setRetentionPolicy(String retentionPolicy);

      abstract Write build();
    }

    public Write withConfiguration(DataSourceConfiguration configuration) {
      checkState(configuration != null, "configuration can not be null");
      return builder().setDataSourceConfiguration(configuration).build();
    }

    public Write withDatabase(String database) {
      return builder().setDatabase(database).build();
    }

    public Write withSslEnabled(boolean sslEnabled) {
      return builder().setSslEnabled(sslEnabled).build();
    }

    public Write withSslInvalidHostNameAllowed(boolean value) {
      return builder().setSslInvalidHostNameAllowed(value).build();
    }

    public Write withNumOfElementsToBatch(int numOfElementsToBatch) {
      return builder().setNumOfElementsToBatch(numOfElementsToBatch).build();
    }

    public Write withFlushDuration(int flushDuration) {
      return builder().setFlushDuration(flushDuration).build();
    }

    public Write withRetentionPolicy(String rp) {
      return builder().setRetentionPolicy(rp).build();
    }

    private class InfluxWriterFn extends DoFn<String, Void> {

      private final Write spec;
      private transient InfluxDB connection;

      InfluxWriterFn(Write write) {
        this.spec = write;
      }

      @Setup
      public void setup() {
        connection =
            getConnection(
                spec.dataSourceConfiguration(), sslInvalidHostNameAllowed(), sslEnabled());
        int flushDuration = spec.flushDuration();
        int numOfBatchPoints = spec.numOfElementsToBatch();
        connection.enableBatch(
            BatchOptions.DEFAULTS.actions(numOfBatchPoints).flushDuration(flushDuration));
        connection.setRetentionPolicy(spec.retentionPolicy());
        connection.setDatabase(spec.database());
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        connection.write(c.element());
      }

      @FinishBundle
      public void finishBundle() {
        connection.flush();
      }

      @Teardown
      public void tearDown() {
        if (connection != null) {
          connection.flush();
          connection.close();
          connection = null;
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(Write.this);
      }
    }
  }

  private static OkHttpClient.Builder getUnsafeOkHttpClient() {
    try {
      // Create a trust manager that does not validate certificate chains
      final TrustManager[] trustAllCerts =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public void checkClientTrusted(
                  java.security.cert.X509Certificate[] chain, String authType) {}

              @Override
              public void checkServerTrusted(
                  java.security.cert.X509Certificate[] chain, String authType) {}

              @Override
              public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[] {};
              }
            }
          };

      // Install the all-trusting trust manager
      final SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      // Create an ssl socket factory with our all-trusting manager
      final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
      builder.hostnameVerifier((hostname, session) -> true);
      return builder;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  /** A POJO describing a DataSourceConfiguration such as URL, userName and password. */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {

    @Nullable
    abstract ValueProvider<String> url();

    @Nullable
    abstract ValueProvider<String> userName();

    @Nullable
    abstract ValueProvider<String> password();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUserName(ValueProvider<String> userName);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(String url, String userName, String password) {
      checkState(url != null, "url can not be null");
      checkState(userName != null, "userName can not be null");
      checkState(password != null, "password can not be null");

      return create(
          ValueProvider.StaticValueProvider.of(url),
          ValueProvider.StaticValueProvider.of(userName),
          ValueProvider.StaticValueProvider.of(password));
    }

    public static DataSourceConfiguration create(
        ValueProvider<String> url, ValueProvider<String> userName, ValueProvider<String> password) {
      checkState(url != null, "url can not be null");
      checkState(userName != null, "userName can not be null");
      checkState(password != null, "password can not be null");

      return new AutoValue_InfluxDbIO_DataSourceConfiguration.Builder()
          .setUrl(url)
          .setUserName(userName)
          .setPassword(password)
          .build();
    }

    public DataSourceConfiguration withUsername(String userName) {
      return withUsername(ValueProvider.StaticValueProvider.of(userName));
    }

    public DataSourceConfiguration withUsername(ValueProvider<String> userName) {
      return builder().setUserName(userName).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DataSourceConfiguration withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    public DataSourceConfiguration withUrl(String url) {
      return withPassword(ValueProvider.StaticValueProvider.of(url));
    }

    public DataSourceConfiguration withUrl(ValueProvider<String> url) {
      return builder().setPassword(url).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {

      builder.addIfNotNull(DisplayData.item("url", url()));
      builder.addIfNotNull(DisplayData.item("userName", userName()));
      builder.addIfNotNull(DisplayData.item("password", password()));
    }
  }

  private static class DataSourceProviderFromDataSourceConfiguration
      implements SerializableFunction<Void, DataSourceConfiguration>, HasDisplayData {
    private final DataSourceConfiguration config;
    private static DataSourceProviderFromDataSourceConfiguration instance;

    private DataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
      this.config = config;
    }

    public static SerializableFunction<Void, DataSourceConfiguration> of(
        DataSourceConfiguration config) {
      if (instance == null) {
        instance = new DataSourceProviderFromDataSourceConfiguration(config);
      }
      return instance;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }

    @Override
    public DataSourceConfiguration apply(Void input) {
      return config;
    }
  }

  private static List<ShardInformation> getDBShardedInformation(
      String database,
      DataSourceConfiguration configuration,
      boolean sslInvalidHostNameAllowed,
      boolean sslEnabled) {
    String query = "show shards";
    DBShardInformation dbInfo = new DBShardInformation();
    try (InfluxDB connection =
        getConnection(configuration, sslInvalidHostNameAllowed, sslEnabled)) {
      QueryResult result = connection.query(new Query(query));
      List<Result> results = result.getResults();
      for (Result res : results) {
        for (Series series : res.getSeries()) {
          dbInfo.loadShardInformation(database, series);
        }
      }
    }
    Collections.sort(dbInfo.getShardInformation(database));
    return dbInfo.getShardInformation(database);
  }

  private static boolean checkDatabase(
      String dbName,
      DataSourceConfiguration configuration,
      boolean sslInvalidHostNameAllowed,
      boolean sslEnabled) {
    try (InfluxDB connection =
        getConnection(configuration, sslInvalidHostNameAllowed, sslEnabled)) {
      QueryResult result = connection.query(new Query("SHOW DATABASES"));
      List<Series> results = result.getResults().get(0).getSeries();
      for (Series series : results) {
        List<List<Object>> values = series.getValues();
        for (List<Object> listObj : values) {
          for (Object dataObj : listObj) {
            if (dataObj.toString().equals(dbName)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  public static InfluxDB getConnection(
      DataSourceConfiguration configuration,
      boolean sslInvalidHostNameAllowed,
      boolean sslEnabled) {
    if (sslInvalidHostNameAllowed && sslEnabled) {
      return InfluxDBFactory.connect(
          configuration.url().get(),
          configuration.userName().get(),
          configuration.password().get(),
          getUnsafeOkHttpClient());
    } else {
      return InfluxDBFactory.connect(
          configuration.url().get(),
          configuration.userName().get(),
          configuration.password().get());
    }
  }
}
