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

import com.google.common.base.Preconditions;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.bson.types.ObjectId;
import org.joda.time.Instant;


/**
  * IO to read and write data on MongoDB GridFS.
 * <p>
 * <h3>Reading from MongoDB via GridFS</h3>
 * <p>
 * <p>MongoDbGridFSIO source returns a bounded collection of String as {@code PCollection<String>}.
 * <p>
 * <p>To configure the MongoDB source, you have to provide the connection URI, the database name
 * and the bucket name. The following example illustrates various options for configuring the
 * source:</p>
 * <p>
 * <pre>{@code
 *
 * pipeline.apply(MongoDbGridFSIO.read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withBucket("my-bucket"))
 *
 * }</pre>
 *
 * <p>The source also accepts an optional configuration: {@code withQueryFilter()} allows you to
 * define a JSON filter to get subset of files in the database.</p>
 *
 * <p>There is also an optional {@code ParseCallback} that can be specified that can be used to
 * parse the InputStream into objects usable with Beam.  By default, MongoDbGridFSIO will parse
 * into Strings, splitting on line breaks and using the uploadDate of the file as the timestamp.
 */
public class MongoDbGridFSIO {

  /**
   *
   * @param <T>
   */
  public interface Parser<T> extends Serializable {
    public void parse(GridFSDBFile input, DoFn<?, T>.ProcessContext result) throws IOException;
  }

  /**
   *
   */
  public static class StringParser implements Parser<String> {
    static final StringParser INSTANCE = new StringParser();

    @Override
    public void parse(GridFSDBFile input, DoFn<?, String>.ProcessContext result)
        throws IOException {
      final Instant time = new Instant(input.getUploadDate().getTime());
      try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input.getInputStream()))) {
        String line = reader.readLine();
        while (line != null) {
          result.outputWithTimestamp(line, time);
          line = reader.readLine();
        }
      }
    }
  }

  /** Read data from GridFS. */
  public static Read<String> read() {
    return new Read<String>(new Read.BoundedGridFSSource<String>(null, null, null, null,
                            StringParser.INSTANCE, null));
  }

  static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    public Read<T> withUri(String uri) {
      return new Read<T>(new BoundedGridFSSource<T>(uri, options.database,
                                           options.bucket, options.filterJson,
                                           options.parser, null));
    }

    public Read<T> withDatabase(String database) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, database,
                                           options.bucket, options.filterJson,
                                           options.parser, null));
    }

    public Read<T> withBucket(String bucket) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, options.database, bucket,
          options.filterJson, options.parser, null));
    }

    public <X> Read<X> withParser(Parser<X> f) {
      return new Read<X>(new BoundedGridFSSource<X>(options.uri, options.database,
          options.bucket, options.filterJson, f, null));
    }

    public Read<T> withQueryFilter(String filterJson) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, options.database,
          options.bucket, filterJson, options.parser, null));
    }

    private final BoundedGridFSSource<T> options;

    Read(BoundedGridFSSource<T> options) {
      this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PCollection<T> apply(PBegin input) {
      org.apache.beam.sdk.io.Read.Bounded<ObjectId> unbounded =
          org.apache.beam.sdk.io.Read.from(options);
      PCollection<T> output = input.getPipeline().apply(unbounded)
          .apply(ParDo.of(new DoFn<ObjectId, T>() {
            Mongo mongo;
            GridFS gridfs;
            @org.apache.beam.sdk.transforms.DoFn.Setup
            public void setup() {
              mongo = options.setupMongo();
              gridfs = options.setupGridFS(mongo);
            }
            @org.apache.beam.sdk.transforms.DoFn.Teardown
            public void teardown() {
              mongo.close();
            }
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
              ObjectId oid = c.element();
              GridFSDBFile file = gridfs.find(oid);
              options.parser.parse(file, c);
            }
          }));
      if (options.parser == StringParser.INSTANCE) {
        output.setCoder((Coder<T>) StringUtf8Coder.of());
      }
      return output;
    }

    static class BoundedGridFSSource<T> extends BoundedSource<ObjectId> {
      @Nullable
      private final String uri;
      @Nullable
      private final String database;
      @Nullable
      private final String bucket;
      @Nullable
      private final String filterJson;
      @Nullable
      private List<ObjectId> objectIds;

      private Parser<T> parser;

      BoundedGridFSSource(String uri, String database,
                          String bucket, String filterJson,
                          Parser<T> parser,
                          List<ObjectId> objectIds) {
        this.uri = uri;
        this.database = database;
        this.bucket = bucket;
        this.objectIds = objectIds;
        this.parser = parser;
        this.filterJson = filterJson;
      }
      private Mongo setupMongo() {
        return uri == null ? new Mongo() : new Mongo(new MongoURI(uri));
      }
      private GridFS setupGridFS(Mongo mongo) {
        DB db = database == null ? mongo.getDB("gridfs") : mongo.getDB(database);
        return bucket == null ? new GridFS(db) : new GridFS(db, bucket);
      }
      private DBCursor createCursor(GridFS gridfs) {
        if (filterJson != null) {
          DBObject query = (DBObject) JSON.parse(filterJson);
          return gridfs.getFileList(query).sort(null);
        }
        return gridfs.getFileList().sort(null);
      }
      @Override
      public List<? extends BoundedSource<ObjectId>> splitIntoBundles(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        Mongo mongo = setupMongo();
        try {
          GridFS gridfs = setupGridFS(mongo);
          DBCursor cursor = createCursor(gridfs);
          long size = 0;
          List<BoundedGridFSSource<T>> list = new LinkedList<>();
          List<ObjectId> objects = new LinkedList<>();
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            long len = file.getLength();
            if ((size + len) > desiredBundleSizeBytes && !objects.isEmpty()) {
              list.add(new BoundedGridFSSource<T>(uri, database, bucket,
                                                 filterJson, parser,
                                                 objects));
              size = 0;
              objects = new LinkedList<>();
            }
            objects.add((ObjectId) file.getId());
            size += len;
          }
          if (!objects.isEmpty() || list.isEmpty()) {
            list.add(new BoundedGridFSSource<T>(uri, database, bucket,
                                                filterJson, parser,
                                                objects));
          }
          return list;
        } finally {
          mongo.close();
        }
      }

      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        Mongo mongo = setupMongo();
        try {
          GridFS gridfs = setupGridFS(mongo);
          DBCursor cursor = createCursor(gridfs);
          long size = 0;
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            size += file.getLength();
          }
          return size;
        } finally {
          mongo.close();
        }
      }


      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return false;
      }

      @Override
      public org.apache.beam.sdk.io.BoundedSource.BoundedReader<ObjectId> createReader(
          PipelineOptions options) throws IOException {
        List<ObjectId> objs = objectIds;
        if (objs == null) {
          objs = new ArrayList<>();
          Mongo mongo = setupMongo();
          try {
            GridFS gridfs = setupGridFS(mongo);
            DBCursor cursor = createCursor(gridfs);
            while (cursor.hasNext()) {
              GridFSDBFile file = (GridFSDBFile) cursor.next();
              objs.add((ObjectId) file.getId());
            }
          } finally {
            mongo.close();
          }
        }
        return new GridFSReader<T>(this, objs);
      }

      @Override
      public void validate() {
        Preconditions.checkNotNull(parser, "Parser cannot be null");
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.addIfNotNull(DisplayData.item("uri", uri));
        builder.addIfNotNull(DisplayData.item("database", database));
        builder.addIfNotNull(DisplayData.item("bucket", bucket));
        builder.addIfNotNull(DisplayData.item("filterJson", filterJson));
      }

      @Override
      public Coder<ObjectId> getDefaultOutputCoder() {
        return SerializableCoder.of(ObjectId.class);
      }
      static class GridFSReader<T> extends BoundedSource.BoundedReader<ObjectId> {
        final BoundedGridFSSource<T> source;
        final List<ObjectId> objects;

        Iterator<ObjectId> iterator;
        ObjectId current;
        GridFSReader(BoundedGridFSSource<T> s, List<ObjectId> objects) {
          source = s;
          this.objects = objects;
        }

        @Override
        public BoundedSource<ObjectId> getCurrentSource() {
          return source;
        }

        @Override
        public boolean start() throws IOException {
          iterator = objects.iterator();
          return advance();
        }

        @Override
        public boolean advance() throws IOException {
          if (iterator.hasNext()) {
            current = iterator.next();
            return true;
          }
          return false;
        }

        @Override
        public ObjectId getCurrent() throws NoSuchElementException {
          return current;
        }

        @Override
        public void close() throws IOException {
        }
      }
    }
  }
}
