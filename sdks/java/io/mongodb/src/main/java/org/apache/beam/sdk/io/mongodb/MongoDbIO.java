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
package org.apache.beam.sdk.io.mongodb;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on MongoDB.
 *
 * <h3>Reading from MongoDB</h3>
 *
 * <p>MongoDbIO source returns a bounded collection of String as {@code PCollection<String>}.
 * The String is the JSON form of the MongoDB Document.
 *
 * <p>To configure the MongoDB source, you have to provide the connection URI, the database name
 * and the collection name. The following example illustrates various options for configuring the
 * source:
 *
 * <pre>{@code
 *
 * pipeline.apply(MongoDbIO.read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withCollection("my-collection"))
 *   // above three are required configuration, returns PCollection<String>
 *
 *   // rest of the settings are optional
 *
 * }</pre>
 *
 * <p>The source also accepts an optional configuration: {@code withFilter()} allows you to
 * define a JSON filter to get subset of data.</p>
 *
 * <h3>Writing to MongoDB</h3>
 *
 * <p>MongoDB sink supports writing of Document (as JSON String) in a MongoDB.</p>
 *
 * <p>To configure a MongoDB sink, you must specify a connection {@code URI}, a {@code Database}
 * name, a {@code Collection} name. For instance:</p>
 *
 * <pre>{@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(MongoDbIO.write()
 *     .withUri("mongodb://localhost:27017")
 *     .withDatabase("my-database")
 *     .withCollection("my-collection")
 *     .withNumSplits(30))
 *
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class MongoDbIO {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIO.class);

  /** Read data from MongoDB. */
  public static Read read() {
    return new AutoValue_MongoDbIO_Read.Builder()
        .setKeepAlive(true)
        .setMaxConnectionIdleTime(60000)
        .setNumSplits(0)
        .build();
  }

  /** Write data to MongoDB. */
  public static Write write() {
    return new AutoValue_MongoDbIO_Write.Builder()
        .setKeepAlive(true)
        .setMaxConnectionIdleTime(60000)
        .setBatchSize(1024L)
        .build();
  }

  private MongoDbIO() {
  }

  /**
   * A {@link PTransform} to read data from MongoDB.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Document>> {
    @Nullable abstract String uri();
    abstract boolean keepAlive();
    abstract int maxConnectionIdleTime();
    @Nullable abstract String database();
    @Nullable abstract String collection();
    @Nullable abstract String filter();
    abstract int numSplits();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);
      abstract Builder setKeepAlive(boolean keepAlive);
      abstract Builder setMaxConnectionIdleTime(int maxConnectionIdleTime);
      abstract Builder setDatabase(String database);
      abstract Builder setCollection(String collection);
      abstract Builder setFilter(String filter);
      abstract Builder setNumSplits(int numSplits);
      abstract Read build();
    }

    /**
     * Define the location of the MongoDB instances using an URI. The URI describes the hosts to
     * be used and some options.
     *
     * <p>The format of the URI is:
     *
     * <pre>{@code
     * mongodb://[username:password@]host1[:port1]...[,hostN[:portN]]][/[database][?options]]
     * }</pre>
     *
     * <p>Where:
     *   <ul>
     *     <li>{@code mongodb://} is a required prefix to identify that this is a string in the
     *     standard connection format.</li>
     *     <li>{@code username:password@} are optional. If given, the driver will attempt to
     *     login to a database after connecting to a database server. For some authentication
     *     mechanisms, only the username is specified and the password is not, in which case
     *     the ":" after the username is left off as well.</li>
     *     <li>{@code host1} is the only required part of the URI. It identifies a server
     *     address to connect to.</li>
     *     <li>{@code :portX} is optional and defaults to {@code :27017} if not provided.</li>
     *     <li>{@code /database} is the name of the database to login to and thus is only
     *     relevant if the {@code username:password@} syntax is used. If not specified, the
     *     "admin" database will be used by default. It has to be equivalent with the database
     *     you specific with {@link Read#withDatabase(String)}.</li>
     *     <li>{@code ?options} are connection options. Note that if {@code database} is absent
     *     there is still a {@code /} required between the last {@code host} and the {@code ?}
     *     introducing the options. Options are name=value pairs and the pairs are separated by
     *     "{@code &}". The {@code KeepAlive} connection option can't be passed via the URI,
     *     instead you have to use {@link Read#withKeepAlive(boolean)}. Same for the
     *     {@code MaxConnectionIdleTime} connection option via
     *     {@link Read#withMaxConnectionIdleTime(int)}.
     *     </li>
     *   </ul>
     */
    public Read withUri(String uri) {
      checkArgument(uri != null, "MongoDbIO.read().withUri(uri) called with null uri");
      return builder().setUri(uri).build();
    }

    /**
     * Sets whether socket keep alive is enabled.
     */
    public Read withKeepAlive(boolean keepAlive) {
      return builder().setKeepAlive(keepAlive).build();
    }

    /**
     * Sets the maximum idle time for a pooled connection.
     */
    public Read withMaxConnectionIdleTime(int maxConnectionIdleTime) {
      return builder().setMaxConnectionIdleTime(maxConnectionIdleTime).build();
    }

    /**
     * Sets the database to use.
     */
    public Read withDatabase(String database) {
      checkArgument(database != null, "database can not be null");
      return builder().setDatabase(database).build();
    }

    /**
     * Sets the collection to consider in the database.
     */
    public Read withCollection(String collection) {
      checkArgument(collection != null, "collection can not be null");
      return builder().setCollection(collection).build();
    }

    /**
     * Sets a filter on the documents in a collection.
     */
    public Read withFilter(String filter) {
      checkArgument(filter != null, "filter can not be null");
      return builder().setFilter(filter).build();
    }

    /**
     * Sets the user defined number of splits.
     */
    public Read withNumSplits(int numSplits) {
      checkArgument(numSplits >= 0, "invalid num_splits: must be >= 0, but was %d", numSplits);
      return builder().setNumSplits(numSplits).build();
    }

    @Override
    public PCollection<Document> expand(PBegin input) {
      checkArgument(uri() != null, "withUri() is required");
      checkArgument(database() != null, "withDatabase() is required");
      checkArgument(collection() != null, "withCollection() is required");
      return input.apply(org.apache.beam.sdk.io.Read.from(new BoundedMongoDbSource(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("uri", uri()));
      builder.add(DisplayData.item("keepAlive", keepAlive()));
      builder.add(DisplayData.item("maxConnectionIdleTime", maxConnectionIdleTime()));
      builder.add(DisplayData.item("database", database()));
      builder.add(DisplayData.item("collection", collection()));
      builder.addIfNotNull(DisplayData.item("filter", filter()));
      builder.add(DisplayData.item("numSplit", numSplits()));
    }
  }

  /**
   * A MongoDB {@link BoundedSource} reading {@link Document} from  a given instance.
   */
  @VisibleForTesting
  static class BoundedMongoDbSource extends BoundedSource<Document> {
    private Read spec;

    private BoundedMongoDbSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public Coder<Document> getOutputCoder() {
      return SerializableCoder.of(Document.class);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public BoundedReader<Document> createReader(PipelineOptions options) {
      return new BoundedMongoDbReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      try (MongoClient mongoClient = new MongoClient(new MongoClientURI(spec.uri()))) {
        return getEstimatedSizeBytes(mongoClient, spec.database(), spec.collection());
      }
    }

    private long getEstimatedSizeBytes(MongoClient mongoClient,
                                       String database,
                                       String collection) {
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);

      // get the Mongo collStats object
      // it gives the size for the entire collection
      BasicDBObject stat = new BasicDBObject();
      stat.append("collStats", collection);
      Document stats = mongoDatabase.runCommand(stat);

      return stats.get("size", Number.class).longValue();
    }

    @Override
    public List<BoundedSource<Document>> split(long desiredBundleSizeBytes,
                                                PipelineOptions options) {
      try (MongoClient mongoClient = new MongoClient(new MongoClientURI(spec.uri()))) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(spec.database());

        List<Document> splitKeys;
        if (spec.numSplits() > 0) {
          // the user defines his desired number of splits
          // calculate the batch size
          long estimatedSizeBytes = getEstimatedSizeBytes(mongoClient,
              spec.database(), spec.collection());
          desiredBundleSizeBytes = estimatedSizeBytes / spec.numSplits();
        }

        // the desired batch size is small, using default chunk size of 1MB
        if (desiredBundleSizeBytes < 1024L * 1024L) {
          desiredBundleSizeBytes = 1L * 1024L * 1024L;
        }

        // now we have the batch size (provided by user or provided by the runner)
        // we use Mongo splitVector command to get the split keys
        BasicDBObject splitVectorCommand = new BasicDBObject();
        splitVectorCommand.append("splitVector", spec.database() + "." + spec.collection());
        splitVectorCommand.append("keyPattern", new BasicDBObject().append("_id", 1));
        splitVectorCommand.append("force", false);
        // maxChunkSize is the Mongo partition size in MB
        LOG.debug("Splitting in chunk of {} MB", desiredBundleSizeBytes / 1024 / 1024);
        splitVectorCommand.append("maxChunkSize", desiredBundleSizeBytes / 1024 / 1024);
        Document splitVectorCommandResult = mongoDatabase.runCommand(splitVectorCommand);
        splitKeys = (List<Document>) splitVectorCommandResult.get("splitKeys");

        List<BoundedSource<Document>> sources = new ArrayList<>();
        if (splitKeys.size() < 1) {
          LOG.debug("Split keys is low, using an unique source");
          sources.add(this);
          return sources;
        }

        LOG.debug("Number of splits is {}", splitKeys.size());
        for (String shardFilter : splitKeysToFilters(splitKeys, spec.filter())) {
          sources.add(new BoundedMongoDbSource(spec.withFilter(shardFilter)));
        }

        return sources;
      }
    }

    /**
     * Transform a list of split keys as a list of filters containing corresponding range.
     *
     * <p>The list of split keys contains BSon Document basically containing for example:
     * <ul>
     *   <li>_id: 56</li>
     *   <li>_id: 109</li>
     *   <li>_id: 256</li>
     * </ul>
     *
     * <p>This method will generate a list of range filters performing the following splits:
     * <ul>
     *   <li>from the beginning of the collection up to _id 56, so basically data with
     *   _id lower than 56</li>
     *   <li>from _id 57 up to _id 109</li>
     *   <li>from _id 110 up to _id 256</li>
     *   <li>from _id 257 up to the end of the collection, so basically data with _id greater
     *   than 257</li>
     * </ul>
     *
     * @param splitKeys The list of split keys.
     * @param additionalFilter A custom (user) additional filter to append to the range filters.
     * @return A list of filters containing the ranges.
     */
    @VisibleForTesting
    static List<String> splitKeysToFilters(List<Document> splitKeys, String
        additionalFilter) {
      ArrayList<String> filters = new ArrayList<>();
      String lowestBound = null; // lower boundary (previous split in the iteration)
      for (int i = 0; i < splitKeys.size(); i++) {
        String splitKey = splitKeys.get(i).get("_id").toString();
        String rangeFilter;
        if (i == 0) {
          // this is the first split in the list, the filter defines
          // the range from the beginning up to this split
          rangeFilter = String.format("{ $and: [ {\"_id\":{$lte:ObjectId(\"%s\")}}",
              splitKey);
          filters.add(formatFilter(rangeFilter, additionalFilter));
        } else if (i == splitKeys.size() - 1) {
          // this is the last split in the list, the filters define
          // the range from the previous split to the current split and also
          // the current split to the end
          rangeFilter = String.format("{ $and: [ {\"_id\":{$gt:ObjectId(\"%s\"),"
              + "$lte:ObjectId(\"%s\")}}", lowestBound, splitKey);
          filters.add(formatFilter(rangeFilter, additionalFilter));
          rangeFilter = String.format("{ $and: [ {\"_id\":{$gt:ObjectId(\"%s\")}}",
              splitKey);
          filters.add(formatFilter(rangeFilter, additionalFilter));
        } else {
          // we are between two splits
          rangeFilter = String.format("{ $and: [ {\"_id\":{$gt:ObjectId(\"%s\"),"
              + "$lte:ObjectId(\"%s\")}}", lowestBound, splitKey);
          filters.add(formatFilter(rangeFilter, additionalFilter));
        }

        lowestBound = splitKey;
      }
      return filters;
    }

    /**
     * Cleanly format range filter, optionally adding the users filter if specified.
     *
     * @param filter The range filter.
     * @param additionalFilter The users filter. Null if unspecified.
     * @return The cleanly formatted range filter.
     */
    private static String formatFilter(String filter, @Nullable String additionalFilter) {
      if (additionalFilter != null && !additionalFilter.isEmpty()) {
        // user provided a filter, we append the user filter to the range filter
        return String.format("%s,%s ]}", filter, additionalFilter);
      } else {
        // user didn't provide a filter, just cleanly close the range filter
        return String.format("%s ]}", filter);
      }
    }
  }

  private static class BoundedMongoDbReader extends BoundedSource.BoundedReader<Document> {
    private final BoundedMongoDbSource source;

    private MongoClient client;
    private MongoCursor<Document> cursor;
    private Document current;

    public BoundedMongoDbReader(BoundedMongoDbSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      Read spec = source.spec;
      MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
      optionsBuilder.maxConnectionIdleTime(spec.maxConnectionIdleTime());
      optionsBuilder.socketKeepAlive(spec.keepAlive());
      client = new MongoClient(new MongoClientURI(spec.uri(), optionsBuilder));

      MongoDatabase mongoDatabase = client.getDatabase(spec.database());

      MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(spec.collection());

      if (spec.filter() == null) {
        cursor = mongoCollection.find().iterator();
      } else {
        Document bson = Document.parse(spec.filter());
        cursor = mongoCollection.find(bson).iterator();
      }

      return advance();
    }

    @Override
    public boolean advance() {
      if (cursor.hasNext()) {
        current = cursor.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public BoundedMongoDbSource getCurrentSource() {
      return source;
    }

    @Override
    public Document getCurrent() {
      return current;
    }

    @Override
    public void close() {
      try {
        if (cursor != null) {
          cursor.close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing MongoDB cursor", e);
      }
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Error closing MongoDB client", e);
      }
    }

  }

  /**
   * A {@link PTransform} to write to a MongoDB database.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Document>, PDone> {

    @Nullable abstract String uri();
    abstract boolean keepAlive();
    abstract int maxConnectionIdleTime();
    @Nullable abstract String database();
    @Nullable abstract String collection();
    abstract long batchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);
      abstract Builder setKeepAlive(boolean keepAlive);
      abstract Builder setMaxConnectionIdleTime(int maxConnectionIdleTime);
      abstract Builder setDatabase(String database);
      abstract Builder setCollection(String collection);
      abstract Builder setBatchSize(long batchSize);
      abstract Write build();
    }

    /**
     * Define the location of the MongoDB instances using an URI. The URI describes the hosts to
     * be used and some options.
     *
     * <p>The format of the URI is:
     *
     * <pre>{@code
     * mongodb://[username:password@]host1[:port1],...[,hostN[:portN]]][/[database][?options]]
     * }</pre>
     *
     * <p>Where:
     *   <ul>
     *     <li>{@code mongodb://} is a required prefix to identify that this is a string in the
     *     standard connection format.</li>
     *     <li>{@code username:password@} are optional. If given, the driver will attempt to
     *     login to a database after connecting to a database server. For some authentication
     *     mechanisms, only the username is specified and the password is not, in which case
     *     the ":" after the username is left off as well.</li>
     *     <li>{@code host1} is the only required part of the URI. It identifies a server
     *     address to connect to.</li>
     *     <li>{@code :portX} is optional and defaults to {@code :27017} if not provided.</li>
     *     <li>{@code /database} is the name of the database to login to and thus is only
     *     relevant if the {@code username:password@} syntax is used. If not specified, the
     *     "admin" database will be used by default. It has to be equivalent with the database
     *     you specific with {@link Write#withDatabase(String)}.</li>
     *     <li>{@code ?options} are connection options. Note that if {@code database} is absent
     *     there is still a {@code /} required between the last {@code host} and the {@code ?}
     *     introducing the options. Options are name=value pairs and the pairs are separated by
     *     "{@code &}". The {@code KeepAlive} connection option can't be passed via the URI, instead
     *     you have to use {@link Write#withKeepAlive(boolean)}. Same for the
     *     {@code MaxConnectionIdleTime} connection option via
     *     {@link Write#withMaxConnectionIdleTime(int)}.
     *     </li>
     *   </ul>
     */
    public Write withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setUri(uri).build();
    }

    /**
     * Sets whether socket keep alive is enabled.
     */
    public Write withKeepAlive(boolean keepAlive) {
      return builder().setKeepAlive(keepAlive).build();
    }

    /**
     * Sets the maximum idle time for a pooled connection.
     */
    public Write withMaxConnectionIdleTime(int maxConnectionIdleTime) {
      return builder().setMaxConnectionIdleTime(maxConnectionIdleTime).build();
    }

    /**
     * Sets the database to use.
     */
    public Write withDatabase(String database) {
      checkArgument(database != null, "database can not be null");
      return builder().setDatabase(database).build();
    }

    /**
     * Sets the collection where to write data in the database.
     */
    public Write withCollection(String collection) {
      checkArgument(collection != null, "collection can not be null");
      return builder().setCollection(collection).build();
    }

    /**
     * Define the size of the batch to group write operations.
     */
    public Write withBatchSize(long batchSize) {
      checkArgument(batchSize >= 0, "Batch size must be >= 0, but was %d", batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PDone expand(PCollection<Document> input) {
      checkArgument(uri() != null, "withUri() is required");
      checkArgument(database() != null, "withDatabase() is required");
      checkArgument(collection() != null, "withCollection() is required");

      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("uri", uri()));
      builder.add(DisplayData.item("keepAlive", keepAlive()));
      builder.add(DisplayData.item("maxConnectionIdleTime", maxConnectionIdleTime()));
      builder.add(DisplayData.item("database", database()));
      builder.add(DisplayData.item("collection", collection()));
      builder.add(DisplayData.item("batchSize", batchSize()));
    }

    static class WriteFn extends DoFn<Document, Void> {
      private final Write spec;
      private transient MongoClient client;
      private List<Document> batch;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createMongoClient() throws Exception {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.socketKeepAlive(spec.keepAlive());
        builder.maxConnectionIdleTime(spec.maxConnectionIdleTime());
        client = new MongoClient(new MongoClientURI(spec.uri(), builder));
      }

      @StartBundle
      public void startBundle() throws Exception {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws Exception {
        // Need to copy the document because mongoCollection.insertMany() will mutate it
        // before inserting (will assign an id).
        batch.add(new Document(ctx.element()));
        if (batch.size() >= spec.batchSize()) {
          flush();
        }
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        flush();
      }

      private void flush() {
        if (batch.isEmpty()) {
          return;
        }
        MongoDatabase mongoDatabase = client.getDatabase(spec.database());
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(spec.collection());
        mongoCollection.insertMany(batch);
        batch.clear();
      }

      @Teardown
      public void closeMongoClient() throws Exception {
        client.close();
        client = null;
      }
    }
  }
}
