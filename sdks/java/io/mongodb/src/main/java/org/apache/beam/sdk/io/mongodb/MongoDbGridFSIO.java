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
import org.apache.beam.sdk.transforms.PTransform;
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
   * Function for parsing the GridFSDBFile into objects for the PCollection.
   * @param <T>
   */
  public interface ParseCallback<T> extends Serializable {
    /**
     * Each value parsed from the file should be output as an
     * Iterable of Line&lt;T&gt;.  If timestamp is omitted, it will
     * use the uploadDate of the GridFSDBFile.
     */
    public static class Line<T> {
      final Instant timestamp;
      final T value;

      public Line(T value, Instant timestamp) {
        this.value = value;
        this.timestamp = timestamp;
      }
      public Line(T value) {
        this.value = value;
        this.timestamp = null;
      }
    };
    public Iterator<Line<T>> parse(GridFSDBFile input) throws IOException;
  }

  /**
   * Default implementation for parsing the InputStream to collection of
   * strings splitting on the cr/lf.
   */
  private static class StringsParseCallback implements ParseCallback<String> {
    static final StringsParseCallback INSTANCE = new StringsParseCallback();

    @Override
    public Iterator<Line<String>> parse(final GridFSDBFile input) throws IOException {
      final BufferedReader reader =
          new BufferedReader(new InputStreamReader(input.getInputStream()));
      return new Iterator<Line<String>>() {
        String val = reader.readLine();
        @Override
        public boolean hasNext() {
          return val != null;
        }

        @Override
        public Line<String> next() {
          Line<String> l = new Line<String>(val);
          try {
            val = reader.readLine();
          } catch (IOException e) {
            val = null;
          }
          return l;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove not supported");
        }
      };
    }
  }

  /** Read data from GridFS. */
  public static Read<String> read() {
    return new Read<String>(new Read.BoundedGridFSSource<String>(null, null, null, null,
                            StringsParseCallback.INSTANCE, StringUtf8Coder.of()));
  }

  static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    public Read<T> withUri(String uri) {
      return new Read<T>(new BoundedGridFSSource<T>(uri, options.database,
                                           options.bucket, options.filterJson,
                                           options.parser, options.coder));
    }

    public Read<T> withDatabase(String database) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, database,
                                           options.bucket, options.filterJson,
                                           options.parser, options.coder));
    }

    public Read<T> withBucket(String bucket) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, options.database, bucket,
          options.filterJson, options.parser, options.coder));
    }

    public <X> Read<X> withParsingFn(ParseCallback<X> f) {
      return new Read<X>(new BoundedGridFSSource<X>(options.uri, options.database,
          options.bucket, options.filterJson, f, null));
    }

    public Read<T> withCoder(Coder<T> coder) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, options.database,
          options.bucket, options.filterJson, options.parser, coder));
    }

    public Read<T> withQueryFilter(String filterJson) {
      return new Read<T>(new BoundedGridFSSource<T>(options.uri, options.database,
          options.bucket, filterJson, options.parser, options.coder));
    }

    private final BoundedGridFSSource<T> options;

    Read(BoundedGridFSSource<T> options) {
      this.options = options;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      org.apache.beam.sdk.io.Read.Bounded<T> unbounded =
          org.apache.beam.sdk.io.Read.from(options);
      PCollection<T> output = input.getPipeline().apply(unbounded);
      if (options.coder != null) {
        output.setCoder(options.coder);
      }
      return output;
    }

    static class BoundedGridFSSource<T> extends BoundedSource<T> {
      @Nullable
      private final String uri;
      @Nullable
      private final String database;
      @Nullable
      private final String bucket;
      @Nullable
      private final String filterJson;
      @Nullable
      private final ParseCallback<T> parser;
      @Nullable
      private final Coder<T> coder;
      @Nullable
      private List<ObjectId> objectIds;
      private transient Mongo mongo;
      private transient GridFS gridfs;

      BoundedGridFSSource(String uri, String database, String bucket, String filterJson,
          ParseCallback<T> parser, Coder<T> coder) {
        this.uri = uri;
        this.database = database;
        this.bucket = bucket;
        this.parser = parser;
        this.coder = coder;
        this.filterJson = filterJson;
      }
      BoundedGridFSSource(String uri, String database, String bucket, String filterJson,
          ParseCallback<T> parser, Coder<T> coder, List<ObjectId> objectIds) {
        this.uri = uri;
        this.database = database;
        this.bucket = bucket;
        this.parser = parser;
        this.coder = coder;
        this.objectIds = objectIds;
        this.filterJson = filterJson;
      }
      private synchronized void setupGridFS() {
        if (gridfs == null) {
          mongo = uri == null ? new Mongo() : new Mongo(new MongoURI(uri));
          DB db = database == null ? mongo.getDB("gridfs") : mongo.getDB(database);
          gridfs = bucket == null ? new GridFS(db) : new GridFS(db, bucket);
        }
      }
      private synchronized void closeGridFS() {
        if (gridfs != null) {
          gridfs = null;
          mongo.close();
          mongo = null;
        }
      }

      @Override
      public List<? extends BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
          PipelineOptions options) throws Exception {
        try {
          setupGridFS();
          DBCursor cursor;
          if (filterJson != null) {
            DBObject query = (DBObject) JSON.parse(filterJson);
            cursor = gridfs.getFileList(query).sort(null);
          } else {
            cursor = gridfs.getFileList().sort(null);
          }
                    long size = 0;
          List<BoundedGridFSSource<T>> list = new LinkedList<>();
          List<ObjectId> objects = new LinkedList<>();
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            long len = file.getLength();
            if ((size + len) > desiredBundleSizeBytes && !objects.isEmpty()) {
              list.add(new BoundedGridFSSource<T>(uri, database, bucket, filterJson,
                                                  parser, coder, objects));
              size = 0;
              objects = new LinkedList<>();
            }
            objects.add((ObjectId) file.getId());
            size += len;
          }
          if (!objects.isEmpty() || list.isEmpty()) {
            list.add(new BoundedGridFSSource<T>(uri, database, bucket, filterJson,
                                                parser, coder, objects));
          }
          return list;
        } finally {
          closeGridFS();
        }
      }

      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        try {
          setupGridFS();
          DBCursor cursor;
          if (filterJson != null) {
            DBObject query = (DBObject) JSON.parse(filterJson);
            cursor = gridfs.getFileList(query).sort(null);
          } else {
            cursor = gridfs.getFileList().sort(null);
          }
          long size = 0;
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            size += file.getLength();
          }
          return size;
        } finally {
          closeGridFS();
        }
      }

      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return false;
      }

      @Override
      public org.apache.beam.sdk.io.BoundedSource.BoundedReader<T> createReader(
          PipelineOptions options) throws IOException {
        return new GridFSReader(this);
      }

      @Override
      public void validate() {
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.addIfNotNull(DisplayData.item("uri", uri));
        builder.addIfNotNull(DisplayData.item("database", database));
        builder.addIfNotNull(DisplayData.item("bucket", bucket));
        builder.addIfNotNull(DisplayData.item("filterJson", filterJson));
      }

      @SuppressWarnings("unchecked")
      @Override
      public Coder<T> getDefaultOutputCoder() {
        if (coder != null) {
          return coder;
        }
        return (Coder<T>) SerializableCoder.of(Serializable.class);
      }

      class GridFSReader extends org.apache.beam.sdk.io.BoundedSource.BoundedReader<T> {
        final BoundedGridFSSource<T> source;

        Instant timestamp = Instant.now();
        Iterator<ParseCallback.Line<T>> currentIterator;
        ParseCallback.Line<T> currentLine;

        GridFSReader(BoundedGridFSSource<T> source) {
          this.source = source;
        }

        @Override
        public boolean start() throws IOException {
          setupGridFS();
          if (objectIds == null) {
            objectIds = new LinkedList<>();
            DBCursor cursor = gridfs.getFileList().sort(null);
            while (cursor.hasNext()) {
              DBObject ob = cursor.next();
              objectIds.add((ObjectId) ob.get("_id"));
            }
          }
          return advance();
        }

        @Override
        public boolean advance() throws IOException {
          if (currentIterator != null && !currentIterator.hasNext()) {
            objectIds.remove(0);
            currentIterator = null;
          }
          if (currentIterator == null) {
            if (objectIds.isEmpty()) {
              return false;
            }
            ObjectId oid = objectIds.get(0);
            GridFSDBFile file = gridfs.find(oid);
            if (file == null) {
              return false;
            }
            timestamp = new Instant(file.getUploadDate().getTime());
            currentIterator = parser.parse(file);
          }

          if (currentIterator.hasNext()) {
            currentLine = currentIterator.next();
            return true;
          }
          return false;
        }

        @Override
        public BoundedSource<T> getCurrentSource() {
          return source;
        }

        @Override
        public T getCurrent() throws NoSuchElementException {
          if (currentLine != null) {
            return currentLine.value;
          }
          throw new NoSuchElementException();
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
          if (currentLine != null) {
            if (currentLine.timestamp != null) {
              return currentLine.timestamp;
            }
            return timestamp;
          }
          throw new NoSuchElementException();
        }

        @Override
        public void close() throws IOException {
          closeGridFS();
        }
      }
    }
  }
}
