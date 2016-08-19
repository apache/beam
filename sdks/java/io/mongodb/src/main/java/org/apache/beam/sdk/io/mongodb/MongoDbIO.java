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
import org.apache.beam.sdk.values.PInput;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nullable;

/**
 * IO to read and write data on MongoDB.
 *
 * <h3>Reading from MongoDB</h3>
 *
 * <p>MongoDbIO source returns a bounded collection of String as {@code PCollection<String>}.
 * The String is the JSON form of the MongoDB Document.</p>
 *
 * <p>To configure the MongoDB source, you have to provide the connection URI, the database name
 * and the collection name. The following example illustrates various options for configuring the
 * source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(MongoDbIO.read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withCollection("my-collection")
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
 *     .withNumSplits(30)
 *
 * }</pre>
 */
// TODO instead of JSON String, does it make sense to populate the PCollection with BSON Document or
//  DBObject ??
public class MongoDbIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIO.class);

  public static Read read() {
    return new Read();
  }

  public static Write write() {
    return new Write();
  }

  private MongoDbIO() {}

  /**
   * A {@link PTransform} to read data from MongoDB.
   */
  public static class Read extends PTransform<PBegin, PCollection<String>> {

    public Read withUri(String uri) {
      return new Read(uri, database, collection, filter, numSplits);
    }

    public Read withDatabase(String database) {
      return new Read(uri, database, collection, filter, numSplits);
    }

    public Read withCollection(String collection) {
      return new Read(uri, database, collection, filter, numSplits);
    }

    public Read withFilter(String filter) {
      return new Read(uri, database, collection, filter, numSplits);
    }

    public Read withNumSplits(int numSplits) {
      return new Read(uri, database, collection, filter, numSplits);
    }

    private String uri;
    private String database;
    private String collection;
    @Nullable
    private String filter;
    private int numSplits;

    private Read() {}

    private Read(String uri, String database, String collection, String filter, int numSplits) {
      this.uri = uri;
      this.database = database;
      this.collection = collection;
      this.filter = filter;
      this.numSplits = numSplits;
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      org.apache.beam.sdk.io.Read.Bounded bounded = org.apache.beam.sdk.io.
          Read.from(createSource());
      PTransform<PInput, PCollection<String>> transform = bounded;
      return input.getPipeline().apply(transform);
    }

    /**
     * Creates a {@link BoundedSource} with the configuration in {@link Read}.
     */
    @VisibleForTesting
    BoundedSource createSource() {
      return new BoundedMongoDbSource(uri, database, collection, filter, numSplits);
    }

    @Override
    public void validate(PBegin input) {
      Preconditions.checkNotNull(uri, "uri");
      Preconditions.checkNotNull(database, "database");
      Preconditions.checkNotNull(collection, "collection");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("uri", uri));
      builder.addIfNotNull(DisplayData.item("database", database));
      builder.addIfNotNull(DisplayData.item("collection", collection));
      builder.addIfNotNull(DisplayData.item("filter", filter));
      builder.addIfNotNull(DisplayData.item("numSplits", numSplits));
    }

  }

  private static class BoundedMongoDbSource extends BoundedSource {

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
    public boolean producesSortedKeys(PipelineOptions options) {
      return false;
    }

    @Override
    public BoundedReader createReader(PipelineOptions options) {
      return new BoundedMongoDbReader(uri, database, collection, filter, this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      long estimatedByteSize = 0L;
      long totalCfByteSize = 0L;

      MongoClient mongoClient = new MongoClient();
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
      MongoCollection mongoCollection = mongoDatabase.getCollection(collection);

      // get the stats object
      BasicDBObject stat = new BasicDBObject();
      stat.append("collStats", collection);
      Document stats = mongoDatabase.runCommand(stat);
      totalCfByteSize = Long.valueOf(stats.get("size").toString());
      long avgSize = Long.valueOf(stats.get("avgObjSize").toString());
      if (filter != null && !filter.isEmpty()) {
        try {
          estimatedByteSize = getFilterResultByteSize(mongoCollection, avgSize);
        } catch (Exception e) {
          LOGGER.warn("Can't estimate size", e);
        }
      } else {
        estimatedByteSize = totalCfByteSize;
      }
      return estimatedByteSize;
    }

    private long getFilterResultByteSize(MongoCollection mongoCollection, long avgSize)
        throws Exception {
      long totalFilterByteSize = 0L;
      Document bson = Document.parse(filter);
      long countFilter = mongoCollection.count(bson);
      totalFilterByteSize = countFilter * avgSize;
      return totalFilterByteSize;
    }

    @Override
    public List<BoundedSource> splitIntoBundles(long desiredBundleSizeBytes,
                                                PipelineOptions options) {
      long numberSplitsCalculated = 0L;
      List<BoundedSource> listEntireCollection = new ArrayList<>();
      MongoClient mongoClient = new MongoClient();
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
      long collectionEstimatedSize = getEstimatedSizeBytes(options);
      ArrayList splitIDFromMongo;
      double initialRatio = 1.6;
      if (numSplits > 0) {
        numberSplitsCalculated = new Long(numSplits);
      } else if (desiredBundleSizeBytes > 0) {
        numberSplitsCalculated = collectionEstimatedSize / desiredBundleSizeBytes;
        if (numberSplitsCalculated <= 0) {
          numberSplitsCalculated = 1L;
        }
      }
      if (numberSplitsCalculated == 1) {
        listEntireCollection.add(this);
        return listEntireCollection;
      }
      int maxChunkSizeBytes = 64000;
      // get the key ranges with splitVector
      BasicDBObject split = new BasicDBObject();
      split.append("splitVector", database + "." + collection);
      BasicDBObject keyPatternValue = new BasicDBObject();
      keyPatternValue.append("_id", 1);
      split.append("keyPattern", keyPatternValue);
      split.append("force", false);
      split.append("maxChunkSizeBytes", maxChunkSizeBytes);
      Document data = mongoDatabase.runCommand(split);
      splitIDFromMongo = (ArrayList) data.get("splitKeys");
      // ratio between number of splits from mongo and splits defined by runner or user
      double calculatedRatio = (double) splitIDFromMongo.size() / numberSplitsCalculated;
      if (((calculatedRatio < initialRatio)
          && (splitIDFromMongo.size() > numberSplitsCalculated))) {
        ArrayList splitIDCalculated = new ArrayList();
        double mod = splitIDFromMongo.size() % numberSplitsCalculated;
        HashMap<Integer, Integer> idRanges = new HashMap<Integer, Integer>();
        // get the ranges
        for (int i = 0; i < numberSplitsCalculated; i++) {
          if (mod >= 0.0) {
            idRanges.put(i, (int) Math.ceil(calculatedRatio));
          } else {
            idRanges.put(i, (int) Math.round((calculatedRatio * 100) / 100));
          }
          mod--;
        }
        // compute the ranges from the split Vector command
        Object splitId = 0L;
        int indexPosition = 0;
        // isPreviousTwo is used to managed the change of value for the range (from 2 to 1)
        boolean isPreviousTwo = true;
        for (Integer range : idRanges.values()) {
          if (range == 2) {
            splitId = splitIDFromMongo.get(indexPosition + 1);
          } else {
            if (isPreviousTwo) {
              indexPosition++;
              isPreviousTwo = false;
            }
            splitId = splitIDFromMongo.get(indexPosition);
          }
          indexPosition++;
          splitIDCalculated.add(splitId);
        }
        return createSourceList(splitIDCalculated);
      }
      return createSourceList(splitIDFromMongo);
    }

    private List<BoundedSource> createSourceList(ArrayList splitIDList) {
      List<BoundedSource> splitSourceList = new ArrayList<>();
      String lastID = null; // Lower boundary of the first min split
      int listIndex = 0;
      for (final Object splitIdValue : splitIDList) {
        String currentID = splitIdValue.toString();
        String newFilter;
        if (filter != null && !filter.isEmpty()) {
          if (listIndex == 0) {
            newFilter = "{ $and: [ {\"_id\":{$lte:ObjectId(\""
                + currentID + "\")}}, "
                + filter + " ]}";
          } else if (listIndex == (splitIDList.size() - 1)) {
            newFilter = "{ $and: [ {\"_id\":{$gt:ObjectId(\"" + lastID
                + "\")," + "$lt:ObjectId(\"" + currentID
                + "\")}}, " + filter + " ]}";
            splitSourceList.add(new BoundedMongoDbSource
                (uri, database, collection, newFilter, numSplits));
            newFilter = "{ $and: [ {\"_id\":{$gt:ObjectId(\"" + currentID
                + "\")}}, " + filter + " ]}";
          } else {
            newFilter = "{ $and: [ {\"_id\":{$gt:ObjectId(\"" + lastID
                + "\")," + "$lte:ObjectId(\"" + currentID + "\")}}, "
                + filter + " ]}";
          }
        } else {
          if (listIndex == 0) {
            newFilter = "{\"_id\":{$lte:ObjectId(\"" + currentID + "\")}}";
          } else if (listIndex == (splitIDList.size() - 1)) {
            newFilter = "{\"_id\":{$gt:ObjectId(\"" + lastID + "\"),"
                + "$lt:ObjectId(\"" + currentID + "\")}}";
            splitSourceList.add(new BoundedMongoDbSource
                (uri, database, collection, newFilter, numSplits));
            newFilter = "{\"_id\":{$gt:ObjectId(\"" + currentID + "\")}}";
          } else {
            newFilter = "{\"_id\":{$gt:ObjectId(\"" + lastID + "\"),"
                + "$lte:ObjectId(\"" + currentID + "\")}}";
          }
        }
        splitSourceList.add(new BoundedMongoDbSource
            (uri, database, collection, newFilter, numSplits));
        lastID = currentID;
        listIndex++;
      }
      return splitSourceList;
    }
  }

  private static class BoundedMongoDbReader extends BoundedSource.BoundedReader {

    private String uri;
    private String database;
    private String collection;
    private String filter;
    private BoundedSource source;

    private MongoClient client;
    private MongoCursor<Document> cursor;
    private String current;

    public BoundedMongoDbReader(String uri, String database, String collection, String filter,
                                BoundedSource source) {
      this.uri = uri;
      this.database = database;
      this.collection = collection;
      this.filter = filter;
      this.source = source;
    }

    @Override
    public boolean start() {
      client = new MongoClient(new MongoClientURI(uri));

      MongoDatabase mongoDatabase = client.getDatabase(database);

      MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

      if (filter == null) {
        cursor = mongoCollection.find().iterator();
      } else {
        Document bson = Document.parse(filter);
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
    public Object getCurrent() {
      return current;
    }

    @Override
    public void close() {
      try {
        cursor.close();
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
      return new Write(uri, database, collection);
    }

    public Write withDatabase(String database) {
      return new Write(uri, database, collection);
    }

    public Write withCollection(String collection) {
      return new Write(uri, database, collection);
    }

    protected String uri;
    protected String database;
    protected String collection;

    private Write() {}

    private Write(String uri, String database, String collection) {
      this.uri = uri;
      this.database = database;
      this.collection = collection;
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(new MongoDbWriter(uri, database, collection)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      Preconditions.checkNotNull(uri, "uri");
      Preconditions.checkNotNull(database, "database");
      Preconditions.checkNotNull(collection, "collection");
    }

    private static class MongoDbWriter extends DoFn<String, Void> {

      private String uri;
      private String database;
      private String collection;

      private MongoClient client;

      public MongoDbWriter(String uri, String database, String collection) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
      }

      @StartBundle
      public void startBundle(Context c) throws Exception {
        if (client == null) {
          client = new MongoClient(new MongoClientURI(uri));
        }
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws Exception {
        String value = ctx.element();

        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection mongoCollection = mongoDatabase.getCollection(collection);

        mongoCollection.insertOne(Document.parse(value));
      }

      @FinishBundle
      public void finishBundle(Context c) throws Exception {
        client.close();
        client = null;
      }

    }

  }

}
