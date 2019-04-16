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
package org.apache.beam.sdk.io.couchbase;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

/**
 * IO to read and write data on Couchbase.
 *
 * <h3>Reading from Couchbase</h3>
 *
 * <p>{@link CouchbaseIO} provides a source to read data and returns a bounded set of JsonDocument.
 * The {@link JsonDocument} is the JSON form of Couchbase document.
 *
 * <p>The following example illustrates various options for configuring the IO:
 *
 * <pre>{@code
 * pipeline.apply(
 *            CouchbaseIO.<JsonDocument>read()
 *                .withHosts(Arrays.asList("host1", "host2"))
 *                .withHttpPort(8091) // Optional
 *                .withCarrierPort(11210) // Optional
 *                .withBucket("bucket1")
 *                .withPassword("pwd")) // Bucket-level password
 *                .withCoder(SerializableCoder.of(JsonDocument.class)));
 *
 *
 * }</pre>
 */
public class CouchbaseIO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseIO.class);
  private static final int DEFAULT_BATCH_SIZE = 100;

  private CouchbaseIO() {}

  /**
   * Provide a {@link Read} {@link PTransform} to read data from a Couchbase database. Here some
   * default options are provided.
   *
   * @return a {@link PTransform} reading data from Couchbase
   */
  public static <T> Read<T> read() {
    return new AutoValue_CouchbaseIO_Read.Builder<T>().setBatchSize(DEFAULT_BATCH_SIZE).build();
  }

  /** A {@link PTransform} to read data from Couchbase. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<Document>> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer httpPort();

    @Nullable
    abstract Integer carrierPort();

    @Nullable
    abstract String bucket();

    @Nullable
    abstract String password();

    abstract Integer batchSize();

    @Nullable
    abstract Coder<T> coder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);

      abstract Builder<T> setHttpPort(Integer port);

      abstract Builder<T> setCarrierPort(Integer port);

      abstract Builder<T> setBucket(String bucket);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setBatchSize(Integer batchSize);

      abstract Builder<T> setCoder(Coder<T> type);

      abstract Read<T> build();
    }

    /**
     * Define a list of ip to the cluster nodes.
     *
     * @param hosts list of ip address
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    /**
     * Define the http port connecting to Couchbase.
     *
     * @param port the http port
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withHttpPort(int port) {
      checkArgument(port > 0, "httpPort must be > 0, but was: %s", port);
      return builder().setHttpPort(port).build();
    }

    /**
     * Define the carrier port connecting to Couchbase.
     *
     * @param port the carrier port
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withCarrierPort(int port) {
      checkArgument(port > 0, "carrierPort must be > 0, but was: %s", port);
      return builder().setCarrierPort(port).build();
    }

    /**
     * Define the name of bucket.
     *
     * @param bucket the bucket name
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withBucket(String bucket) {
      checkArgument(bucket != null, "bucket can not be null");
      return builder().setBucket(bucket).build();
    }

    /**
     * Define the bucket-level password to the target bucket.
     *
     * @param password password
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * Define the batch size that the reader will fetch each time.
     *
     * @param batchSize batchSize
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withBatchSize(Integer batchSize) {
      checkArgument(batchSize != null, "batchSize can not be null");
      return builder().setBatchSize(batchSize).build();
    }

    /**
     * Define the coder to document.
     *
     * @param coder coder
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "documentCoder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<Document> expand(PBegin input) {
      checkArgument((hosts() != null), "WithHosts() is required");
      checkArgument(bucket() != null, "withBucket() is required");

      return input
          .apply(Create.of((Void) null))
          .apply(ParDo.of(new GenerateOffsetRanges(this))) // 1. Fetch the total number of keys
          .apply(
              ParDo.of(
                  new ReadData(
                      this))) // 2. Each reader is responsible for fetching a portion of data
          .setCoder(new DocumentCoder<>(coder())); // 3. Set up the coder of document
    }
  }

  /**
   * Wrap the user defined coder to be able to encode and decode the concrete type in Runtime.
   *
   * @param <T>
   */
  static class DocumentCoder<T> extends Coder<Document> {

    private Coder<T> coder;

    DocumentCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public void encode(Document value, OutputStream outStream) throws IOException {
      coder.encode((T) value, outStream);
    }

    @Override
    public Document decode(InputStream inStream) throws IOException {
      return (Document) coder.decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return coder.getCoderArguments();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      coder.verifyDeterministic();
    }
  }

  abstract static class CouchbaseDoFn<K, V> extends DoFn<K, V> {
    Cluster cluster;
    Bucket bucket;
    Read spec;

    CouchbaseDoFn(Read spec) {
      this.spec = spec;
    }

    @Setup
    public void connect() {
      DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
      if (spec.httpPort() != null) {
        builder.bootstrapHttpDirectPort(spec.httpPort());
      }
      if (spec.carrierPort() != null) {
        builder.bootstrapCarrierDirectPort(spec.carrierPort());
      }
      cluster = CouchbaseCluster.create(builder.build(), spec.hosts());
      // For Couchbase Server, in the previous version than 5.0, the passwordless bucket can be
      // supported.
      // But after version 5.0, the newly created user should have a username equal to bucket name
      // and a password.
      // For more information, please go to
      // https://docs.couchbase.com/java-sdk/2.7/sdk-authentication-overview.html#legacy-connection-code
      bucket =
          spec.password() == null
              ? cluster.openBucket(spec.bucket())
              : cluster.openBucket(spec.bucket(), spec.password());
    }

    @Teardown
    public void teardown() {
      bucket.close();
      cluster.disconnect();
    }
  }

  static class GenerateOffsetRanges extends CouchbaseDoFn<Void, OffsetRange> {

    GenerateOffsetRanges(Read spec) {
      super(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      // Get the total size of keys
      String query = String.format("SELECT RAW COUNT(META().id) FROM `%s`", bucket.name());
      LOG.debug(query);
      N1qlQueryResult result = bucket.query(N1qlQuery.simple(query));
      if (!result.finalSuccess()) {
        throw new IOException(result.errors().get(0).getString("msg"));
      }
      int itemCount =
          Integer.parseInt(
              new String(result.allRows().get(0).byteValue(), Charset.forName("UTF-8")));

      // Calculate the total size of bundles
      int totalBundle = (int) Math.ceil((double) itemCount / spec.batchSize());

      // Create a set of ranges and populate to the output
      for (int i = 0, offset = 0; i < totalBundle; i++) {
        int lowerBound = offset;
        int upperBound = offset += spec.batchSize();
        if (i == totalBundle - 1) {
          upperBound = itemCount;
        }
        context.output(new OffsetRange(lowerBound, upperBound));
      }
    }
  }

  static class ReadData extends CouchbaseDoFn<OffsetRange, Document> {

    ReadData(Read spec) {
      super(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      OffsetRange range = context.element();
      int lowerBound = (int) range.getFrom();
      int upperBound = (int) range.getTo();

      // Fetch the keys between the range
      String query =
          String.format(
              "SELECT RAW META().id FROM `%s` OFFSET %d LIMIT %d",
              spec.bucket(), lowerBound, upperBound - lowerBound);
      LOG.debug(String.format("Couchbase reader [%d, %d): %s", lowerBound, upperBound, query));
      N1qlQueryResult result = bucket.query(N1qlQuery.simple(query));
      if (!result.finalSuccess()) {
        throw new IOException(result.errors().get(0).getString("msg"));
      }
      List<String> keys =
          result.allRows().stream()
              .map(r -> new String(r.byteValue(), Charset.forName("UTF-8")))
              .map(key -> key.substring(1, key.length() - 1)) // Remove the useless double quotas
              .collect(Collectors.toList());

      // Fetch the documents corresponding to the keys and then output them
      Observable.from(keys)
          .flatMap(
              (Func1<String, Observable<Document>>)
                  id -> bucket.async().get(JsonDocument.create(id)))
          .toList()
          .toBlocking()
          .single()
          .forEach(context::output);
    }
  }
}
