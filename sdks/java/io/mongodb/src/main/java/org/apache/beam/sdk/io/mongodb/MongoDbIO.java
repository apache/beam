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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

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
 * <p>
 * <h3>Reading from MongoDB</h3>
 * <p>
 * <p>MongoDbIO source returns a bounded collection of String as {@code PCollection<String>}.
 * The String is the JSON form of the MongoDB Document.</p>
 * <p>
 * <p>To configure the MongoDB source, you have to provide the connection URI, the database name
 * and the collection name. The following example illustrates various options for configuring the
 * source:</p>
 * <p>
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
 * <p>
 * <p>The source also accepts an optional configuration: {@code withFilter()} allows you to
 * define a JSON filter to get subset of data.</p>
 * <p>
 * <h3>Writing to MongoDB</h3>
 * <p>
 * <p>MongoDB sink supports writing of Document (as JSON String) in a MongoDB.</p>
 * <p>
 * <p>To configure a MongoDB sink, you must specify a connection {@code URI}, a {@code Database}
 * name, a {@code Collection} name. For instance:</p>
 * <p>
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
// TODO instead of JSON String, does it make sense to populate the PCollection with BSON Document or
//  DBObject ??
public class MongoDbIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIO.class);

  /** Read data from MongoDB. */
  public static Read read() {
    return new Read(new BoundedMongoDbSource(null, null, null, null, 0));
  }

  /** Write data to MongoDB. */
  public static Write write() {
    return new Write(new Write.MongoDbWriter(null, null, null, 1024L));
  }

  private MongoDbIO() {
  }

  /**
   * A {@link PTransform} to read data from MongoDB.
   */
  public static class Read extends PTransform<PBegin, PCollection<String>> {

    public Read withUri(String uri) {
      return new Read(source.withUri(uri));
    }

    public Read withDatabase(String database) {
      return new Read(source.withDatabase(database));
    }

    public Read withCollection(String collection) {
      return new Read(source.withCollection(collection));
    }

    public Read withFilter(String filter) {
      return new Read(source.withFilter(filter));
    }

    public Read withNumSplits(int numSplits) {
      return new Read(source.withNumSplits(numSplits));
    }

    private final BoundedMongoDbSource source;

    private Read(BoundedMongoDbSource source) {
      this.source = source;
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(getSource()));
    }

    /**
     * Creates a {@link BoundedSource} with the configuration in {@link Read}.
     */
    @VisibleForTesting
    BoundedSource<String> getSource() {
      return source;
    }

    @Override
    public void validate(PBegin input) {
      source.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      source.populateDisplayData(builder);
    }

  }

  private static class BoundedMongoDbSource extends BoundedSource<String> {

    public BoundedMongoDbSource withUri(String uri) {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    public BoundedMongoDbSource withDatabase(String database) {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    public BoundedMongoDbSource withCollection(String collection) {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    public BoundedMongoDbSource withFilter(String filter) {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    public BoundedMongoDbSource withNumSplits(int numSplits) {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    private final String uri;
    private final String database;
    private final String collection;
    @Nullable
    private final String filter;
    private final int numSplits;

    public BoundedMongoDbSource(String uri, String database, String collection, String filter,
                                int numSplits) {
      this.uri = uri;
      this.database = database;
      this.collection = collection;
      this.filter = filter;
      this.numSplits = numSplits;
    }

    @Override
    public Coder getDefaultOutputCoder() {
      return SerializableCoder.of(String.class);
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(uri, "uri");
      Preconditions.checkNotNull(database, "database");
      Preconditions.checkNotNull(collection, "collection");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("uri", uri));
      builder.add(DisplayData.item("database", database));
      builder.add(DisplayData.item("collection", collection));
      builder.addIfNotNull(DisplayData.item("filter", filter));
      builder.add(DisplayData.item("numSplit", numSplits));
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      return false;
    }

    @Override
    public BoundedReader createReader(PipelineOptions options) {
      return new BoundedMongoDbReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      long estimatedSizeBytes = 0L;

      MongoClient mongoClient = new MongoClient();
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
      MongoCollection mongoCollection = mongoDatabase.getCollection(collection);

      // get the Mongo collStats object
      // it gives the size for the entire collection
      BasicDBObject stat = new BasicDBObject();
      stat.append("collStats", collection);
      Document stats = mongoDatabase.runCommand(stat);
      estimatedSizeBytes = Long.valueOf(stats.get("size").toString());
      return estimatedSizeBytes;
    }

    @Override
    public List<BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
                                                PipelineOptions options) {
      MongoClient mongoClient = new MongoClient();
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);

      List<Document> splitKeys = null;
      if (numSplits > 0) {
        // the user defines his desired number of splits
        // calculate the batch size
        long estimatedSizeBytes = getEstimatedSizeBytes(options);
        desiredBundleSizeBytes = estimatedSizeBytes / numSplits;
      }

      // the desired batch size is small, using default chunk size of 1MB
      if (desiredBundleSizeBytes < 1024 * 1024) {
        desiredBundleSizeBytes = 1 * 1024 * 1024;
      }

      // now we have the batch size (provided by user or provided by the runner)
      // we use Mongo splitVector command to get the split keys
      BasicDBObject splitVectorCommand = new BasicDBObject();
      splitVectorCommand.append("splitVector", database + "." + collection);
      splitVectorCommand.append("keyPattern", new BasicDBObject().append("_id", 1));
      splitVectorCommand.append("force", false);
      // maxChunkSize is the Mongo partition size in MB
      LOGGER.debug("Splitting in chunk of {} MB", desiredBundleSizeBytes / 1024 / 1024);
      splitVectorCommand.append("maxChunkSize", desiredBundleSizeBytes / 1024 / 1024);
      Document splitVectorCommandResult = mongoDatabase.runCommand(splitVectorCommand);
      splitKeys = (List<Document>) splitVectorCommandResult.get("splitKeys");

      List<BoundedSource<String>> sources = new ArrayList<>();
      if (splitKeys.size() < 1) {
        LOGGER.debug("Split keys is low, using an unique source");
        sources.add(this);
        return sources;
      }

      LOGGER.debug("Number of splits is {}", splitKeys.size());
      for (String shardFilter : splitKeysToFilters(splitKeys, filter)) {
        sources.add(this.withFilter(shardFilter));
      }

      return sources;
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
     * </p>
     *
     * This method will generate a list of range filters performing the following splits:
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
    private static List<String> splitKeysToFilters(List<Document> splitKeys, String
        additionalFilter) {
      ArrayList<String> filters = new ArrayList<>();
      String lowestBound = null; // lower boundary (previous split in the iteration)
      for (int i = 0; i < splitKeys.size(); i++) {
        String splitKey = splitKeys.get(i).toString();
        String rangeFilter = null;
        if (i == 0) {
          // this is the first split in the list, the filter defines
          // the range from the beginning up to this split
          rangeFilter = String.format("{ $and: [ {\"_id\":{$lte:Objectd(\"%s\")}}",
              splitKey);
        } else if (i == splitKeys.size() - 1) {
          // this is the last split in the list, the filter defines
          // the range from the split up to the end
          rangeFilter = String.format("{ $and: [ {\"_id\":{$gt:ObjectId(\"%s\")}}",
              splitKey);
        } else {
          // we are between two splits
          rangeFilter = String.format("{ $and: [ {\"_id\":{$gt:ObjectId(\"%s\"),"
              + "$lte:ObjectId(\"%s\")}}", lowestBound, splitKey);
        }
        if (additionalFilter != null && !additionalFilter.isEmpty()) {
          // user provided a filter, we append the user filter to the range filter
          rangeFilter = String.format("%s,%s ]}", rangeFilter, additionalFilter);
        } else {
          // user didn't provide a filter, just cleany close the range filter
          rangeFilter = String.format("%s ]}", rangeFilter);
        }

        filters.add(rangeFilter);

        lowestBound = splitKey;
      }
      return filters;
    }
  }

  private static class BoundedMongoDbReader extends BoundedSource.BoundedReader<String> {

    private final BoundedMongoDbSource source;

    private MongoClient client;
    private MongoCursor<Document> cursor;
    private String current;

    public BoundedMongoDbReader(BoundedMongoDbSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      client = new MongoClient(new MongoClientURI(source.uri));

      MongoDatabase mongoDatabase = client.getDatabase(source.database);

      MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(source.collection);

      if (source.filter == null) {
        cursor = mongoCollection.find().iterator();
      } else {
        Document bson = Document.parse(source.filter);
        cursor = mongoCollection.find(bson).iterator();
      }

      return advance();
    }

    @Override
    public boolean advance() {
      if (cursor.hasNext()) {
        current = cursor.next().toJson();
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
      return current;
    }

    @Override
    public void close() {
      try {
        if (cursor != null) {
          cursor.close();
        }
      } catch (Exception e) {
        LOGGER.warn("Error closing MongoDB cursor", e);
      }
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing MongoDB client", e);
      }
    }

  }

  /**
   * A {@link PTransform} to write to a MongoDB database.
   */
  public static class Write extends PTransform<PCollection<String>, PDone> {

    public Write withUri(String uri) {
      return new Write(writer.withUri(uri));
    }

    public Write withDatabase(String database) {
      return new Write(writer.withDatabase(database));
    }

    public Write withCollection(String collection) {
      return new Write(writer.withCollection(collection));
    }

    public Write withBatchSize(long batchSize) {
      return new Write(writer.withBatchSize(batchSize));
    }

    private final MongoDbWriter writer;

    private Write(MongoDbWriter writer) {
      this.writer = writer;
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(writer));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      writer.validate();
    }

    private static class MongoDbWriter extends DoFn<String, Void> {

      private final String uri;
      private final String database;
      private final String collection;
      private final long batchSize;

      private MongoClient client;
      private List<Document> batch;

      public MongoDbWriter(String uri, String database, String collection, long batchSize) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.batchSize = batchSize;
      }

      public MongoDbWriter withUri(String uri) {
        return new MongoDbWriter(uri, database, collection, batchSize);
      }

      public MongoDbWriter withDatabase(String database) {
        return new MongoDbWriter(uri, database, collection, batchSize);
      }

      public MongoDbWriter withCollection(String collection) {
        return new MongoDbWriter(uri, database, collection, batchSize);
      }

      public MongoDbWriter withBatchSize(long batchSize) {
        return new MongoDbWriter(uri, database, collection, batchSize);
      }

      public void validate() {
        Preconditions.checkNotNull(uri, "uri");
        Preconditions.checkNotNull(database, "database");
        Preconditions.checkNotNull(collection, "collection");
        Preconditions.checkNotNull(batchSize, "batchSize");
      }

      @Setup
      public void createMongoClient() throws Exception {
        client = new MongoClient(new MongoClientURI(uri));
      }

      @StartBundle
      public void startBundle(Context ctx) throws Exception {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws Exception {
        String value = ctx.element();

        batch.add(Document.parse(ctx.element()));
        if (batch.size() >= batchSize) {
          finishBundle(ctx);
        }
      }

      @FinishBundle
      public void finishBundle(Context ctx) throws Exception {
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection mongoCollection = mongoDatabase.getCollection(collection);

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
