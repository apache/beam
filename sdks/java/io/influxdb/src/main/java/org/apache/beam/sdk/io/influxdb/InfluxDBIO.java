package org.apache.beam.sdk.io.influxdb;

import com.google.auto.value.AutoValue;
import okhttp3.OkHttpClient;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.*;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.*;
import java.io.Serializable;
import java.security.cert.CertificateException;
import java.util.*;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 *  IO to read and write to InfluxDB.
  <h3>Reading from InfluxDB datasource</h3>
        *
        * <p>InfluxDBIO source returns a bounded collection of {@code String} as a {@code PCollection<String>}.
        *
        * <p>To configure the InfluxDB source, you have to provide a {@link DataSourceConfiguration} using<br>
        * {@link DataSourceConfiguration#create(String, String, String)}(durl, username and password).
        * Optionally, {@link DataSourceConfiguration#withUsername(String)} and {@link
        * DataSourceConfiguration#withPassword(String)} allows you to define username and password.
         *
         * <p>For example:
        *
        * <pre>{@code
        * PCollection<Stringn> collection = pipeline.apply(InfluxDBIO.read()
        *   .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
        *          "https://localhost:8086","username","password"))
        *   .withDatabase("metrics")
        *   .withRetentionPolicy("autogen")
        *   .withSslInvalidHostNameAllowed(true)
        *   withSslEnabled(true));
        * }</pre>
        *
        * <p> For example (Read from query):
        * <pre>{@code
        * PCollection<Stringn> collection = pipeline.apply(InfluxDBIO.read()
        *   .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
        *          "https://localhost:8086","username","password"))
        *   .withDatabase("metrics")
        *   .withQuery("Select * from cpu")
        *   .withRetentionPolicy("autogen")
        *   .withSslInvalidHostNameAllowed(true)
        *   withSslEnabled(true));
        * }</pre>
        * <h3>Writing to Influx datasource</h3>
        *
        * <p>InfluxDB sink supports writing records into a database. It writes a {@link PCollection} to the
        * database by converting each T. The T should implement  getLineProtocol() from {@link LineProtocolConvertable}.
        *
        * <p>Like the source, to configure the sink, you have to provide a {@link DataSourceConfiguration}.
        *
        * <pre>{@code
        * pipeline
        *   .apply(...)
        *   .apply(InfluxDb.write()
        *      .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
        *            "https://localhost:8086","username","password"))
        *   .withRetentionPolicy("autogen")
        *   .withDatabase("metrics")
        *   .withSslInvalidHostNameAllowed(true)
        *   withSslEnabled(true));
        *    );
        * }</pre>
 * *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class InfluxDBIO {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBIO.class);

    public static Write write() {
        return new AutoValue_InfluxDBIO_Write.Builder()
                .build();
    }
    public static Read read() {
        return new AutoValue_InfluxDBIO_Read.Builder()
                .build();
    }

    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
        @Nullable
        abstract Boolean sslInvalidHostNameAllowed();
        @Nullable
        abstract String retentionPolicy();
        @Nullable
        abstract String database();
        @Nullable
        abstract String query();
        @Nullable
        abstract Boolean sslEnabled();
        @Nullable
        abstract DataSourceConfiguration dataSourceConfiguration();
        @Nullable
        abstract List<String> metric();
        abstract Builder builder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);
            abstract Builder setDatabase(String database);
            abstract Builder setSslInvalidHostNameAllowed(Boolean value);
            abstract Builder setRetentionPolicy(String retentionPolicy);
            abstract Builder setQuery(String query);
            abstract Builder setSslEnabled(Boolean sslEnabled);
            abstract Builder setMetric(List<String> metric);

            abstract Read build();
        }

        /** Reads from the InfluxDB instance indicated by the given configuration. */
        public Read withDataSourceConfiguration(DataSourceConfiguration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return builder().setDataSourceConfiguration(configuration).build();
        }

        /** Reads from the specified database. */
        public Read withDatabase(String database) {
            return builder().setDatabase(database).setDataSourceConfiguration(dataSourceConfiguration()).build();
        }
        /** Reads from the specified query. */
        public Read withQuery(String query) {
            return builder().setQuery(query).build();
        }

        public Read withMetric(List<String> metric){
            return builder().setMetric(metric).build();
        }
        public Read withSslEnabled(boolean sslEnabled){
            return builder().setSslEnabled(sslEnabled).build();
        }

        public Read withSslInvalidHostNameAllowed(Boolean value) {
            return builder().setSslInvalidHostNameAllowed(value).build();
        }
        public Read withRetentionPolicy(String rp) {
            return builder().setRetentionPolicy(rp).build();
        }
        @Override
        public PCollection<String> expand(PBegin input) {
            checkArgument(dataSourceConfiguration() != null, "withConfiguration() is required");
            checkArgument(query() != null || database() !=null, "withDatabase() or withQuery() is required");
            if(database() !=null) {
                try (InfluxDB connection = getConnection(dataSourceConfiguration(), sslInvalidHostNameAllowed(), sslEnabled())) {
                    checkArgument(
                            connection.databaseExists(database()), "Database %s does not exist", database());

                }
            }
            return input.apply(
                    org.apache.beam.sdk.io.Read.from(new InfluxDBSource (this)));

        }
        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.addIfNotNull(DisplayData.item("dataSourceConfiguration", dataSourceConfiguration().toString()));
            builder.addIfNotNull(DisplayData.item("database", database()));
            builder.addIfNotNull(DisplayData.item("retentionPolicy", retentionPolicy()));
            builder.addIfNotNull(DisplayData.item("sslEnabled", sslEnabled()));
            builder.addIfNotNull(DisplayData.item("query", query()));
            builder.addIfNotNull(DisplayData.item("sslInvalidHostNameAllowed", sslInvalidHostNameAllowed()));
        }

    }
    static class InfluxDBSource extends BoundedSource<String> {
        private final Read spec;
        InfluxDBSource(Read read) {
            this.spec = read;
        }
        @Override
        public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
            int size = 0;
            try (InfluxDB connection = getConnection(spec.dataSourceConfiguration(), spec.sslInvalidHostNameAllowed(),spec.sslEnabled())) {
                connection.setDatabase(spec.database());
                QueryResult queryResult = connection.query(new Query(getQueryToRun(spec),spec.database()));
                if(queryResult != null) {
                    List databaseNames =
                            queryResult.getResults().get(0).getSeries().get(0).getValues();
                    if (databaseNames != null) {
                        Iterator var4 = databaseNames.iterator();
                        while (var4.hasNext()) {
                            List database = (List) var4.next();
                            size += database.size();
                        }
                    }
                }
            }
            LOG.info(
                    "Estimated number of elements {} for database {}",
                    size,
                    spec.database());
            return size;
        }

        /**
         *
         * @param desiredElementsInABundle
         * @param options
         * @return
         * @throws Exception
         */
        @Override
        public List<? extends BoundedSource<String>> split(
                long desiredElementsInABundle, PipelineOptions options) throws Exception {
            List<BoundedSource<String>> sources = new  ArrayList<BoundedSource<String>>();
            if(spec.metric() !=null && spec.metric().size()>1) {
                for (String metric : spec.metric()) {
                    sources.add(new InfluxDBSource(spec.withMetric(Arrays.asList(metric))));
                }
            }else {
                sources.add(this);
            }
            checkArgument(!sources.isEmpty(), "No primary shard found");
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
            return SerializableCoder.of(String.class);
        }

    }
    public static String getQueryToRun(Read spec){
        if (spec.query() == null) {
            return "SELECT * FROM " + String.join(",",spec.metric());
        }
        return spec.query();
    }

    private static InfluxDB getConnection(DataSourceConfiguration configuration, boolean sslInvalidHostNameAllowed, boolean sslEnabled){
        if (sslInvalidHostNameAllowed && sslEnabled)
            return InfluxDBFactory.connect(configuration.getUrl().get(),
                    configuration.getUsername().get(),
                    configuration.getPassword().get(),
                    getUnsafeOkHttpClient());
        else
            return InfluxDBFactory.connect(configuration.getUrl().get(),
                    configuration.getUsername().get(),
                    configuration.getPassword().get());
    }

    private static class BoundedInfluxDbReader extends
            BoundedSource.BoundedReader<String> {
        private final InfluxDBIO.InfluxDBSource source;
        private Iterator cursor;
        private List current;

        public BoundedInfluxDbReader(InfluxDBIO.InfluxDBSource source) {
            this.source = source;
        }

        @Override
        public boolean start() {
            InfluxDBIO.Read spec = source.spec;
            try(InfluxDB influxDB =  getConnection(spec.dataSourceConfiguration(),
                    spec.sslInvalidHostNameAllowed(),spec.sslEnabled())){
                if (spec.database() !=null)
                    influxDB.setDatabase(spec.database());
                String query = getQueryToRun(spec);
                LOG.error("BoundedInfluxDbReader.start() ==> " + query);

                QueryResult queryResult = influxDB.query(new Query(query, spec.database()));

                List databaseNames =
                        queryResult.getResults().get(0).getSeries().get(0).getValues();

                if (databaseNames != null) {
                    cursor = databaseNames.iterator();
                }
            }
            return advance();
        }

        @Override
        public boolean advance()  {
            if (cursor.hasNext()) {
                current = (List) cursor.next();
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
        public String getCurrent() throws NoSuchElementException {
            return current.toString();
        }

        @Override
        public void close() {
            return;
        }
    }
    @AutoValue
    public abstract static class Write extends PTransform<PCollection<String>, PDone> {

        @Override
        public PDone expand(PCollection<String> input) {
            checkArgument(dataSourceConfiguration() != null, "withConfiguration() is required");
            checkArgument(database() != null && !database().isEmpty(), "withDatabase() is required");
            try (InfluxDB connection = getConnection(dataSourceConfiguration(), sslInvalidHostNameAllowed(),sslEnabled())) {
                checkArgument(
                        connection.databaseExists(database()), "Database %s does not exist", database());
            }
            input.apply(ParDo.of(new InfluxWriterFn(this)));
            return PDone.in(input.getPipeline());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.addIfNotNull(DisplayData.item("dataSourceConfiguration", dataSourceConfiguration().toString()));
            builder.addIfNotNull(DisplayData.item("database", database()));
            builder.addIfNotNull(DisplayData.item("retentionPolicy", retentionPolicy()));
            builder.addIfNotNull(DisplayData.item("sslEnabled", sslEnabled()));
            builder.addIfNotNull(DisplayData.item("sslInvalidHostNameAllowed", sslInvalidHostNameAllowed()));
            builder.addIfNotNull(DisplayData.item("noOfElementsToBatch",noOfElementsToBatch()));
            builder.addIfNotNull(DisplayData.item("flushDuration",flushDuration()));
        }
        @Nullable
        abstract String database();
        @Nullable
        abstract String retentionPolicy();
        @Nullable
        abstract Boolean sslInvalidHostNameAllowed();
        @Nullable
        abstract Boolean sslEnabled();
        @Nullable
        abstract Integer noOfElementsToBatch();
        @Nullable
        abstract Integer flushDuration();
        @Nullable
        abstract DataSourceConfiguration dataSourceConfiguration();
        abstract Builder builder();

        @AutoValue.Builder
        abstract static class Builder
        {
            abstract Builder setDataSourceConfiguration(DataSourceConfiguration configuration);
            abstract Builder setDatabase(String database);
            abstract Builder setSslInvalidHostNameAllowed(Boolean value);
            abstract Builder setNoOfElementsToBatch(Integer noOfElementsToBatch);
            abstract Builder setFlushDuration(Integer flushDuration);
            abstract Builder setSslEnabled(Boolean sslEnabled);
            abstract Builder setRetentionPolicy(String retentionPolicy);
            abstract Write build();
        }

        public Write withConfiguration(DataSourceConfiguration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return builder().setDataSourceConfiguration(configuration).build();
        }

        public Write withDatabase(String database) {
            return builder().setDatabase(database).build();
        }

        public Write withSslEnabled(boolean sslEnabled){
            return builder().setSslEnabled(sslEnabled).build();
        }
        public Write withSslInvalidHostNameAllowed(Boolean value) {
            return builder().setSslInvalidHostNameAllowed(value).build();
        }
        public Write withNoOfElementsToBatch(Integer noOfElementsToBatch) {
            return builder().setNoOfElementsToBatch(noOfElementsToBatch).build();
        }
        public Write withFlushDuration(Integer flushDuration) {
            return builder().setFlushDuration(flushDuration).build();
        }
        public Write withRetentionPolicy(String rp) {
            return builder().setRetentionPolicy(rp).build();
        }



        private class InfluxWriterFn<T> extends DoFn<T, Void> {

            private final Write spec;
            private InfluxDB connection;

            InfluxWriterFn(Write write) {
                this.spec = write;
            }

            @Setup
            public void setup() throws Exception {
                connection = getConnection(spec.dataSourceConfiguration(), sslInvalidHostNameAllowed(), sslEnabled());
                int flushDuration = spec.flushDuration() != null  ? spec.flushDuration(): DEFAULT_FLUSH_DURATION;
                int noOfBatchPoints = spec.noOfElementsToBatch() != null ? spec.noOfElementsToBatch() : DEFAULT_NUMBER_OF_DURATION;
                connection.enableBatch(BatchOptions.DEFAULTS.actions(noOfBatchPoints).flushDuration(flushDuration));
                connection.setDatabase(spec.database());
            }

            @StartBundle
            public void startBundle(StartBundleContext c) {
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
//                    Point point = Point.measurementByPOJO(c.element().getClass()).addFieldsFromPOJO(c.element()).build();
                    connection.write(c.element().toString());
            }

            @FinishBundle
            public void finishBundle() throws Exception {
                connection.flush();
            }

            @Teardown
            public void tearDown() throws Exception {
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

            private final Integer DEFAULT_NUMBER_OF_DURATION = 1000;
            private final Integer DEFAULT_FLUSH_DURATION = 100;

        }
    }

    public static OkHttpClient.Builder getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });

            return builder;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * A POJO describing a {}, either providing directly a or all
     * properties allowing to create a {}.
     */
    @AutoValue
    public abstract static class DataSourceConfiguration implements Serializable {

        @Nullable
        abstract ValueProvider<String> getUrl();

        @Nullable
        abstract ValueProvider<String> getUsername();

        @Nullable
        abstract ValueProvider<String> getPassword();

        abstract Builder builder();


        @AutoValue.Builder
        abstract static class Builder {

            abstract Builder setUrl(ValueProvider<String> url);

            abstract Builder setUsername(ValueProvider<String> username);

            abstract Builder setPassword(ValueProvider<String> password);

            abstract DataSourceConfiguration build();
        }

        public static DataSourceConfiguration create(String url, String username, String password) {
            checkArgument(url != null, "url can not be null");
            checkArgument(username != null, "username can not be null");
            checkArgument(password != null, "password can not be null");

            return create(
                    ValueProvider.StaticValueProvider.of(url),
                    ValueProvider.StaticValueProvider.of(username),
                    ValueProvider.StaticValueProvider.of(password));
        }

        public static DataSourceConfiguration create( ValueProvider<String> url,
                                                      ValueProvider<String> username,ValueProvider<String> password ) {
            checkArgument(url != null, "url can not be null");
            checkArgument(username != null, "username can not be null");
            checkArgument(password != null, "password can not be null");

            return new AutoValue_InfluxDBIO_DataSourceConfiguration.Builder()
                    .setUrl(url)
                    .setUsername(username)
                    .setPassword(password)
                    .build();
        }

        public DataSourceConfiguration withUsername(String username) {
            return withUsername(ValueProvider.StaticValueProvider.of(username));
        }

        public DataSourceConfiguration withUsername(ValueProvider<String> username) {
            return builder().setUsername(username).build();
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

            builder.addIfNotNull(DisplayData.item("url", getUrl()));
            builder.addIfNotNull(DisplayData.item("username", getUsername()));
            builder.addIfNotNull(DisplayData.item("password", getPassword()));

        }
    }
    private static class DataSourceProviderFromDataSourceConfiguration
            implements SerializableFunction<Void, DataSourceConfiguration>, HasDisplayData {
        private final DataSourceConfiguration config;
        private static DataSourceProviderFromDataSourceConfiguration instance;

        private DataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
            this.config = config;
        }

        public static SerializableFunction<Void, DataSourceConfiguration> of(DataSourceConfiguration config) {
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


}

