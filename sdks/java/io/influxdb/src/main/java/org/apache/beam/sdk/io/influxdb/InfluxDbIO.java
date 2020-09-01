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
import static org.influxdb.BatchOptions.DEFAULT_BUFFER_LIMIT;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write from InfluxDB.
 *
 * <h3>Reading from InfluxDB</h3>
 *
 * <p>InfluxDB return a bounded collection of String as {@code PCollection<String>}. The String
 * follows the line protocol
 * (https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/). To Configure
 * the InfluxDB source, you whave to provide the connection URL, the credentials to connect to
 * InfluxDB and the Database name
 *
 * <pre>{@code
 *  pipeline.apply(
 *    InfluxDB.read("https://influxdb", "userName", "password", "database")
 *      .withQuery("select * from metric");
 *      //Reads data based on the query from the InfluxDB
 * }
 *
 * <p> The source also accepts optional configuration: {@code withRetentionPolicy()}  an</p>
 *
 *
 * <h3>Writing to InfluxDB</h3>
 *
 * <p>InfluxDB sink supports writing data (as line protocol)  to InfluxDB
 * To configure a InfluxDB sink, you must specify a URL {@code InfluxDBURL}, {@code userName}, {@code password}, {@code database}
 * <pre>{@code
 * pipeleine
 *    .apply(...)
 *    .appply(InfluxDB.write(https://influxdb", "userName", "password", "database")
 *
 * }
 * </pre>
 * </pre>
 */
@Experimental(Kind.SOURCE_SINK)
public class InfluxDbIO {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDbIO.class);

  private static final String defaultRetentionPolicy = "autogen";

  /** Disallow construction of utility class. */
  private InfluxDbIO() {}

  public static Write write(String influxDbUrl, String username, String password, String database) {
    return new AutoValue_InfluxDbIO_Write.Builder()
        .setDataSourceConfiguration(
            DataSourceConfiguration.create(
                StaticValueProvider.of(influxDbUrl),
                StaticValueProvider.of(username),
                StaticValueProvider.of(password)))
        .setDatabase(database)
        .setRetentionPolicy(defaultRetentionPolicy)
        .setDisableCertificateValidation(false)
        .setBatchSize(DEFAULT_BUFFER_LIMIT)
        .setConsistencyLevel(ConsistencyLevel.QUORUM)
        .build();
  }

  public static Read read(String influxDbUrl, String username, String password, String database) {
    return new AutoValue_InfluxDbIO_Read.Builder()
        .setDataSourceConfiguration(
            DataSourceConfiguration.create(
                StaticValueProvider.of(influxDbUrl),
                StaticValueProvider.of(username),
                StaticValueProvider.of(password)))
        .setDatabase(database)
        .setDisableCertificateValidation(false)
        .setRetentionPolicy(defaultRetentionPolicy)
        .build();
  }

  ///////////////////////// Read  \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to read from InfluxDB metric or data related to query. See {@link
   * InfluxDB} for more information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    abstract String retentionPolicy();

    abstract String database();

    abstract @Nullable String query();

    abstract DataSourceConfiguration dataSourceConfiguration();

    abstract @Nullable String metric();

    abstract @Nullable String fromDateTime();

    abstract @Nullable String toDateTime();

    abstract boolean disableCertificateValidation();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);

      abstract Builder setDatabase(String database);

      abstract Builder setRetentionPolicy(String retentionPolicy);

      abstract Builder setQuery(String query);

      abstract Builder setToDateTime(String toDateTime);

      public abstract Builder setDisableCertificateValidation(boolean value);

      abstract Builder setFromDateTime(String fromDateTime);

      abstract Builder setMetric(@Nullable String metric);

      abstract Read build();
    }
    /** Read metric data till the toDateTime. * */
    public Read withToDateTime(String toDateTime) {
      return builder().setToDateTime(toDateTime).build();
    }
    /** Read metric data from the fromDateTime. * */
    public Read withFromDateTime(String fromDateTime) {
      return builder().setFromDateTime(fromDateTime).build();
    }
    /** Sets the metric to use. * */
    public Read withMetric(@Nullable String metric) {
      return builder().setMetric(metric).build();
    }
    /** Disable SSL certification validation. * */
    public Read withDisableCertificateValidation(boolean disableCertificateValidation) {
      return builder().setDisableCertificateValidation(disableCertificateValidation).build();
    }
    /** Sets the retention policy to use. * */
    public Read withRetentionPolicy(String retentionPolicy) {
      return builder().setRetentionPolicy(retentionPolicy).build();
    }
    /** Sets the query to use. * */
    public Read withQuery(String query) {
      return builder().setQuery(query).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(new InfluxDBSource(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      dataSourceConfiguration().populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("metric", metric()));
      builder.addIfNotNull(DisplayData.item("retentionPolicy", retentionPolicy()));
      builder.addIfNotNull(DisplayData.item("database", database()));
      builder.addIfNotNull(DisplayData.item("fromDateTime", fromDateTime()));
      builder.addIfNotNull(DisplayData.item("toDateTime", toDateTime()));
      builder.addIfNotNull(
          DisplayData.item("disableCertificateValidation", disableCertificateValidation()));
      builder.addIfNotNull(DisplayData.item("query", query()));
    }
  }
  /** A InfluxDb {@link BoundedSource} reading {@link String} from a given instance. */
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
          getConnection(spec.dataSourceConfiguration(), spec.disableCertificateValidation())) {
        String query = spec.query();
        final String db = spec.database();
        if (query == null) {
          checkState(spec.metric() != null, "Both query and metrics are empty");
          query = String.format("SELECT * FROM %s.%s", spec.retentionPolicy(), spec.metric());
        }
        QueryResult result = connection.query(new Query("EXPLAIN " + query, db));
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
      long size = 0L;
      while (numOfBlocksValueIterator.hasNext() && sizeOfBlocksValueIterator.hasNext()) {
        size = size + (numOfBlocksValueIterator.next() * sizeOfBlocksValueIterator.next());
      }
      return size;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredElementsInABundle, PipelineOptions options) throws Exception {
      List<ShardInformation> shardInfo =
          getDBShardedInformation(
              spec.database(), spec.dataSourceConfiguration(), spec.disableCertificateValidation());
      List<BoundedSource<String>> sources = new ArrayList<>();
      String metric = spec.metric();
      if (spec.query() == null) {
        for (ShardInformation sInfo : shardInfo) {
          sources.add(
              new InfluxDBSource(
                  spec.withMetric(metric)
                      .withRetentionPolicy(sInfo.getRetentionPolicy())
                      .withToDateTime(sInfo.getStartTime())
                      .withFromDateTime(sInfo.getEndTime())));
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
    String query = spec.query();
    if (query == null) {
      if (spec.toDateTime() != null && spec.fromDateTime() != null) {
        String retVal =
            String.format(
                "SELECT * FROM %s.%s WHERE time >= '%s' and time <= '%s'",
                spec.retentionPolicy(), spec.metric(), spec.toDateTime(), spec.fromDateTime());
        return retVal;
      } else {
        return String.format("SELECT * FROM %s.%s", spec.retentionPolicy(), spec.metric());
      }
    } else {
      return query;
    }
  }

  private static class BoundedInfluxDbReader extends BoundedSource.BoundedReader<String> {
    private final InfluxDbIO.InfluxDBSource source;
    private Iterator<Result> resultIterator;
    private Iterator<Series> seriesIterator;
    private Iterator<List<Object>> valuesIterator;
    private List<Object> current;

    public BoundedInfluxDbReader(InfluxDbIO.InfluxDBSource source) {
      this.source = source;
      this.seriesIterator = Collections.emptyIterator();
      this.resultIterator = Collections.emptyIterator();
      this.valuesIterator = Collections.emptyIterator();
      this.current = Collections.EMPTY_LIST;
    }

    @Override
    public boolean start() {
      InfluxDbIO.Read spec = source.spec;
      try (InfluxDB influxDB =
          getConnection(spec.dataSourceConfiguration(), spec.disableCertificateValidation())) {
        final String db = spec.database();
        influxDB.setDatabase(spec.database());
        influxDB.setRetentionPolicy(spec.retentionPolicy());
        String query = getQueryToRun(spec);
        final QueryResult queryResult = influxDB.query(new Query(query, db));
        resultIterator = queryResult.getResults().iterator();
        if (resultIterator.hasNext()) {
          Result result = resultIterator.next();
          if (result != null && result.getSeries() != null) {
            seriesIterator = result.getSeries().iterator();
          }
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
    public BoundedSource<String> getCurrentSource() {
      return source;
    }

    @Override
    public String getCurrent() {
      return current.toString();
    }

    @Override
    public void close() {}
  }

  /** A {@link PTransform} to write to a InfluxDB datasource. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Override
    public PDone expand(PCollection<String> input) {
      checkState(
          checkDatabase(database(), dataSourceConfiguration(), disableCertificateValidation()),
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
      builder.addIfNotNull(DisplayData.item("batchSize", batchSize()));
      builder.addIfNotNull(DisplayData.item("consistencyLevel", consistencyLevel().value()));
    }

    abstract String database();

    abstract String retentionPolicy();

    abstract int batchSize();

    abstract boolean disableCertificateValidation();

    abstract ConsistencyLevel consistencyLevel();

    abstract DataSourceConfiguration dataSourceConfiguration();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);

      abstract Builder setDatabase(String database);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setConsistencyLevel(ConsistencyLevel consistencyLevel);

      public abstract Builder setDisableCertificateValidation(boolean value);

      abstract Builder setRetentionPolicy(String retentionPolicy);

      abstract Write build();
    }
    /** Number of elements to batch. * */
    public Write withBatchSize(int batchSize) {
      return builder().setBatchSize(batchSize).build();
    }
    /** Disable SSL certification validation. * */
    public Write withDisableCertificateValidation(boolean disableCertificateValidation) {
      return builder().setDisableCertificateValidation(disableCertificateValidation).build();
    }
    /** Sets the retention policy to use. * */
    public Write withRetentionPolicy(String rp) {
      return builder().setRetentionPolicy(rp).build();
    }
    /**
     * Sets the consistency level to use. ALL("all") Write succeeds only if write reached all
     * cluster members. ANY("any") Write succeeds if write reached at least one cluster members.
     * ONE("one") Write succeeds if write reached at least one cluster members. QUORUM("quorum")
     * Write succeeds only if write reached a quorum of cluster members.
     */
    public Write withConsistencyLevel(ConsistencyLevel consistencyLevel) {
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    static class InfluxWriterFn extends DoFn<String, Void> {

      private final Write spec;
      private List<String> batch;

      InfluxWriterFn(Write write) {
        this.spec = write;
      }

      @StartBundle
      public void startBundle() {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        batch.add(context.element());
        if (batch.size() >= spec.batchSize()) {
          flush();
        }
      }

      @FinishBundle
      public void finishBundle() {
        flush();
      }

      @Teardown
      public void tearDown() {
        flush();
      }

      private void flush() {
        if (batch.isEmpty()) {
          return;
        }
        try (InfluxDB connection =
            getConnection(spec.dataSourceConfiguration(), spec.disableCertificateValidation())) {
          connection.setDatabase(spec.database());
          connection.enableBatch();
          connection.setConsistency(spec.consistencyLevel());
          connection.write(batch);
        } catch (InfluxDBException exception) {
          throw exception;
        }
        batch.clear();
      }
    }
  }

  /** A POJO describing a DataSourceConfiguration such as URL, userName and password. */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {

    abstract ValueProvider<String> url();

    abstract ValueProvider<String> userName();

    abstract ValueProvider<String> password();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUserName(ValueProvider<String> userName);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(
        ValueProvider<String> url, ValueProvider<String> userName, ValueProvider<String> password) {
      return new AutoValue_InfluxDbIO_DataSourceConfiguration.Builder()
          .setUrl(url)
          .setUserName(userName)
          .setPassword(password)
          .build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("url", url()));
      builder.add(DisplayData.item("userName", userName()));
      builder.add(DisplayData.item("password", password()));
    }
  }

  private static List<ShardInformation> getDBShardedInformation(
      String database, DataSourceConfiguration configuration, boolean disableCertificateValidation)
      throws Exception {
    String query = "SHOW SHARDS";
    DBShardInformation dbInfo = new DBShardInformation();
    try (InfluxDB connection = getConnection(configuration, disableCertificateValidation)) {
      QueryResult result = connection.query(new Query(query));
      List<Result> results = result.getResults();
      for (Result res : results) {
        for (Series series : res.getSeries()) {
          dbInfo.loadShardInformation(database, series);
        }
      }
    }
    Collections.sort(dbInfo.getShardInformation(database), new ShardInformationByStartDate());
    return dbInfo.getShardInformation(database);
  }

  private static boolean checkDatabase(
      String dbName, DataSourceConfiguration configuration, boolean disableCertificateValidation) {

    try (InfluxDB connection = getConnection(configuration, disableCertificateValidation)) {
      connection.setDatabase(dbName);
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
      DataSourceConfiguration configuration, boolean disableCertificateValidation) {
    if (!disableCertificateValidation) {
      return InfluxDBFactory.connect(
          configuration.url().get(),
          configuration.userName().get(),
          configuration.password().get());
    } else {
      return InfluxDBFactory.connect(
          configuration.url().get(),
          configuration.userName().get(),
          configuration.password().get(),
          getUnsafeOkHttpClient());
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
}
