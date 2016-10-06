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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
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
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * IO to read and write data on MongoDB GridFS.
 *
 * <h3>Reading from MongoDB via GridFS</h3>
 *
 * <p>MongoDbGridFSIO source returns a bounded collection of Objects as {@code PCollection<T>}.
 *
 * <p>To configure the MongoDB GridFS source, you can provide the connection URI, the database name
 * and the bucket name.  If unspecified, the default values from the GridFS driver are used.</p>
 *
 * <p>The following example illustrates various options for configuring the
 * source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(MongoDbGridFSIO.<String>read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withBucket("my-bucket"))
 *
 * }</pre>
 *
 * <p>The source also accepts an optional configuration: {@code withQueryFilter()} allows you to
 * define a JSON filter to get subset of files in the database.</p>
 *
 * <p>There is also an optional {@code Parser} (and associated {@code Coder}) that can be
 * specified that can be used to parse the InputStream into objects usable with Beam. By default,
 * MongoDbGridFSIO will parse into Strings, splitting on line breaks and using the uploadDate of
 * the file as the timestamp.
 * When using a parser that outputs with custom timestamps, you may also need to specify
 * the allowedTimestampSkew option.</p>
 */
public class MongoDbGridFSIO {

  /**
   * Callback for the parser to use to submit data.
   */
  public interface ParserCallback<T> extends Serializable {
    /**
     * Output the object.  The default timestamp will be the GridFSDBFile
     * creation timestamp.
     * @param output
     */
    void output(T output);

    /**
     * Output the object using the specified timestamp.
     * @param output
     * @param timestamp
     */
    void output(T output, Instant timestamp);
  }

  /**
   * Interface for the parser that is used to parse the GridFSDBFile into
   * the appropriate types.
   * @param <T>
   */
  public interface Parser<T> extends Serializable {
    void parse(GridFSDBFile input, ParserCallback<T> callback) throws IOException;
  }

  /**
   * For the default {@code Read<String>} case, this is the parser that is used to
   * split the input file into Strings. It uses the timestamp of the file
   * for the event timestamp.
   */
  private static final Parser<String> TEXT_PARSER = new Parser<String>() {
    @Override
    public void parse(GridFSDBFile input, ParserCallback<String> callback)
        throws IOException {
      final Instant time = new Instant(input.getUploadDate().getTime());
      try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input.getInputStream()))) {
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          callback.output(line, time);
        }
      }
    }
  };

  /** Read data from GridFS. Default behavior with String. */
  public static Read<String> read() {
    return new AutoValue_MongoDbGridFSIO_Read.Builder<String>().build()
        .withParser(TEXT_PARSER).withCoder(StringUtf8Coder.of());
  }

  /**
   * A {@link PTransform} to read data from MongoDB GridFS.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Nullable abstract String uri();
    @Nullable abstract String database();
    @Nullable abstract String bucket();
    @Nullable abstract Parser<T> parser();
    @Nullable abstract Coder<T> coder();
    @Nullable abstract Duration skew();
    @Nullable abstract String filter();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setUri(String uri);
      abstract Builder<T> setDatabase(String database);
      abstract Builder<T> setBucket(String bucket);
      abstract Builder<T> setParser(Parser<T> parser);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setSkew(Duration skew);
      abstract Builder<T> setFilter(String filter);
      abstract Read<T> build();
    }

    public Read<T> withUri(String uri) {
      checkNotNull(uri);
      return toBuilder().setUri(uri).build();
    }

    public Read<T> withDatabase(String database) {
      checkNotNull(database);
      return toBuilder().setDatabase(database).build();
    }

    public Read<T> withBucket(String bucket) {
      checkNotNull(bucket);
      return toBuilder().setBucket(bucket).build();
    }

    public <X> Read<X> withParser(Parser<X> parser) {
      checkNotNull(parser);
      Builder<X> builder = (Builder<X>) toBuilder();
      return builder.setParser(parser).setCoder(null).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkNotNull(coder);
      return toBuilder().setCoder(coder).build();
    }

    public Read<T> withSkew(Duration skew) {
      return toBuilder().setSkew(skew == null ? Duration.ZERO : skew).build();
    }

    public Read<T> withFilter(String filter) {
      return toBuilder().setFilter(filter).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("uri", uri()));
      builder.addIfNotNull(DisplayData.item("database", database()));
      builder.addIfNotNull(DisplayData.item("bucket", bucket()));
      builder.addIfNotNull(DisplayData.item("parser", parser().getClass().getName()));
      builder.addIfNotNull(DisplayData.item("coder", coder().getClass().getName()));
      builder.addIfNotNull(DisplayData.item("skew", skew()));
      builder.addIfNotNull(DisplayData.item("filter", filter()));
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      final BoundedGridFSSource source = new BoundedGridFSSource(this, null);
      org.apache.beam.sdk.io.Read.Bounded<ObjectId> objectIds =
          org.apache.beam.sdk.io.Read.from(source);
      PCollection<T> output = input.getPipeline().apply(objectIds)
          .apply(ParDo.of(new DoFn<ObjectId, T>() {
            Mongo mongo;
            GridFS gridfs;

            @Setup
            public void setup() {
              mongo = source.setupMongo();
              gridfs = source.setupGridFS(mongo);
            }

            @Teardown
            public void teardown() {
              mongo.close();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws IOException {
              ObjectId oid = c.element();
              GridFSDBFile file = gridfs.find(oid);
              parser().parse(file, new ParserCallback<T>() {
                @Override
                public void output(T output, Instant timestamp) {
                  checkNotNull(timestamp);
                  c.outputWithTimestamp(output, timestamp);
                }

                @Override
                public void output(T output) {
                  c.output(output);
                }
              });
            }

            @Override
            public Duration getAllowedTimestampSkew() {
              return skew();
            }
          }));
      if (coder() != null) {
        output.setCoder(coder());
      }
      return output;
    }

    /**
     * A {@link BoundedSource} for MongoDB GridFS.
     */
    protected static class BoundedGridFSSource extends BoundedSource<ObjectId> {

      private Read spec;

      @Nullable
      private List<ObjectId> objectIds;

      BoundedGridFSSource(Read spec, List<ObjectId> objectIds) {
        this.spec = spec;
        this.objectIds = objectIds;
      }

      private Mongo setupMongo() {
        return spec.uri() == null ? new Mongo() : new Mongo(new MongoURI(spec.uri()));
      }

      private GridFS setupGridFS(Mongo mongo) {
        DB db = spec.database() == null ? mongo.getDB("gridfs") : mongo.getDB(spec.database());
        return spec.bucket() == null ? new GridFS(db) : new GridFS(db, spec.bucket());
      }

      private DBCursor createCursor(GridFS gridfs) {
        if (spec.filter() != null) {
          DBObject query = (DBObject) JSON.parse(spec.filter());
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
          List<BoundedGridFSSource> list = new ArrayList<>();
          List<ObjectId> objects = new ArrayList<>();
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            long len = file.getLength();
            if ((size + len) > desiredBundleSizeBytes && !objects.isEmpty()) {
              list.add(new BoundedGridFSSource(spec, objects));
              size = 0;
              objects = new ArrayList<>();
            }
            objects.add((ObjectId) file.getId());
            size += len;
          }
          if (!objects.isEmpty() || list.isEmpty()) {
            list.add(new BoundedGridFSSource(spec, objects));
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
      public BoundedSource.BoundedReader<ObjectId> createReader(
          PipelineOptions options) throws IOException {
        return new GridFSReader(this, objectIds);
      }

      @Override
      public void validate() {
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        spec.populateDisplayData(builder);
      }

      @Override
      public Coder<ObjectId> getDefaultOutputCoder() {
        return SerializableCoder.of(ObjectId.class);
      }

      static class GridFSReader extends BoundedSource.BoundedReader<ObjectId> {
        final BoundedGridFSSource source;

        /* When split into bundles, this records the ObjectId's of the files for
         * this bundle.  Otherwise, this is null.  When null, a DBCursor of the
         * files is used directly to avoid having the ObjectId's queried and
         * loaded ahead of time saving time and memory.
         */
        @Nullable
        final List<ObjectId> objects;

        Mongo mongo;
        DBCursor cursor;
        Iterator<ObjectId> iterator;
        ObjectId current;

        GridFSReader(BoundedGridFSSource source, List<ObjectId> objects) {
          this.source = source;
          this.objects = objects;
        }

        @Override
        public BoundedSource<ObjectId> getCurrentSource() {
          return source;
        }

        @Override
        public boolean start() throws IOException {
          if (objects == null) {
            mongo = source.setupMongo();
            GridFS gridfs = source.setupGridFS(mongo);
            cursor = source.createCursor(gridfs);
          } else {
            iterator = objects.iterator();
          }
          return advance();
        }

        @Override
        public boolean advance() throws IOException {
          if (iterator != null && iterator.hasNext()) {
            current = iterator.next();
            return true;
          } else if (cursor != null && cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            current = (ObjectId) file.getId();
            return true;
          }
          current = null;
          return false;
        }

        @Override
        public ObjectId getCurrent() throws NoSuchElementException {
          if (current == null) {
            throw new NoSuchElementException();
          }
          return current;
        }

        public Instant getCurrentTimestamp() throws NoSuchElementException {
          if (current == null) {
            throw new NoSuchElementException();
          }
          long time = current.getTimestamp();
          time *= 1000L;
          return new Instant(time);
        }

        @Override
        public void close() throws IOException {
          if (mongo != null) {
            mongo.close();
          }
        }
      }
    }
  }
}
