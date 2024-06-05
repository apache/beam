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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
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
 * and the bucket name. If unspecified, the default values from the GridFS driver are used.
 *
 * <p>The following example illustrates various options for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(MongoDbGridFSIO.<String>read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withBucket("my-bucket"))
 * }</pre>
 *
 * <p>The source also accepts an optional configuration: {@code withQueryFilter()} allows you to
 * define a JSON filter to get subset of files in the database.
 *
 * <p>There is also an optional {@code Parser} (and associated {@code Coder}) that can be specified
 * that can be used to parse the InputStream into objects usable with Beam. By default,
 * MongoDbGridFSIO will parse into Strings, splitting on line breaks and using the uploadDate of the
 * file as the timestamp. When using a parser that outputs with custom timestamps, you may also need
 * to specify the allowedTimestampSkew option.
 *
 * <h3>Writing to MongoDB via GridFS</h3>
 *
 * <p>MongoDBGridFS supports writing of data to a file in a MongoDB GridFS collection.
 *
 * <p>To configure a MongoDB GridFS sink, you can provide the connection URI, the database name and
 * the bucket name. You must also provide the filename to write to. Another optional parameter is
 * the GridFS file chunkSize.
 *
 * <p>For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(MongoDbGridFSIO.write()
 *     .withUri("mongodb://localhost:27017")
 *     .withDatabase("my-database")
 *     .withBucket("my-bucket")
 *     .withChunkSize(256000L)
 *     .withFilename("my-output.txt"))
 *
 * }</pre>
 *
 * <p>There is also an optional argument to the {@code create()} method to specify a writer that is
 * used to write the data to the OutputStream. By default, it writes UTF-8 strings to the file
 * separated with line feeds.
 */
public class MongoDbGridFSIO {

  /** Callback for the parser to use to submit data. */
  public interface ParserCallback<T> extends Serializable {
    /** Output the object. The default timestamp will be the GridFSDBFile creation timestamp. */
    void output(T output);

    /** Output the object using the specified timestamp. */
    void output(T output, Instant timestamp);
  }

  /** Interface for the parser that is used to parse the GridFSDBFile into the appropriate types. */
  public interface Parser<T> extends Serializable {
    void parse(GridFSDBFile input, ParserCallback<T> callback) throws IOException;
  }

  /**
   * For the default {@code Read<String>} case, this is the parser that is used to split the input
   * file into Strings. It uses the timestamp of the file for the event timestamp.
   */
  private static final Parser<String> TEXT_PARSER =
      (input, callback) -> {
        final Instant time = new Instant(input.getUploadDate().getTime());
        try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(input.getInputStream(), StandardCharsets.UTF_8))) {
          for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            callback.output(line, time);
          }
        }
      };

  /** Read data from GridFS. Default behavior with String. */
  public static Read<String> read() {
    return new AutoValue_MongoDbGridFSIO_Read.Builder<String>()
        .setParser(TEXT_PARSER)
        .setCoder(StringUtf8Coder.of())
        .setConnectionConfiguration(ConnectionConfiguration.create())
        .setSkew(Duration.ZERO)
        .build();
  }

  /** Write data to GridFS. Default behavior with String. */
  public static Write<String> write() {
    return new AutoValue_MongoDbGridFSIO_Write.Builder<String>()
        .setConnectionConfiguration(ConnectionConfiguration.create())
        .setWriteFn(
            (output, outStream) -> {
              outStream.write(output.getBytes(StandardCharsets.UTF_8));
              outStream.write('\n');
            })
        .build();
  }

  public static <T> Write<T> write(WriteFn<T> fn) {
    return new AutoValue_MongoDbGridFSIO_Write.Builder<T>()
        .setWriteFn(fn)
        .setConnectionConfiguration(ConnectionConfiguration.create())
        .build();
  }

  /** Encapsulate the MongoDB GridFS connection logic. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    @Pure
    abstract @Nullable String uri();

    @Pure
    abstract @Nullable String database();

    @Pure
    abstract @Nullable String bucket();

    static ConnectionConfiguration create() {
      return new AutoValue_MongoDbGridFSIO_ConnectionConfiguration(null, null, null);
    }

    static ConnectionConfiguration create(
        @Nullable String uri, @Nullable String database, @Nullable String bucket) {
      return new AutoValue_MongoDbGridFSIO_ConnectionConfiguration(uri, database, bucket);
    }

    MongoClient setupMongo() {
      return uri() == null ? new MongoClient() : new MongoClient(new MongoClientURI(uri()));
    }

    GridFS setupGridFS(MongoClient mongo) {
      DB db = database() == null ? mongo.getDB("gridfs") : mongo.getDB(database());
      return bucket() == null ? new GridFS(db) : new GridFS(db, bucket());
    }
  }

  /** A {@link PTransform} to read data from MongoDB GridFS. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Pure
    abstract ConnectionConfiguration connectionConfiguration();

    @Pure
    abstract @Nullable Parser<T> parser();

    @Pure
    abstract @Nullable Coder<T> coder();

    @Pure
    abstract @Nullable Duration skew();

    @Pure
    abstract @Nullable String filter();

    @Pure
    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration connection);

      abstract Builder<T> setParser(Parser<T> parser);

      abstract Builder<T> setCoder(@Nullable Coder<T> coder);

      abstract Builder<T> setSkew(Duration skew);

      abstract Builder<T> setFilter(String filter);

      abstract Read<T> build();
    }

    public Read<T> withUri(String uri) {
      Preconditions.checkArgumentNotNull(uri);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              uri, connectionConfiguration().database(), connectionConfiguration().bucket());
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public Read<T> withDatabase(String database) {
      Preconditions.checkArgumentNotNull(database);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              connectionConfiguration().uri(), database, connectionConfiguration().bucket());
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public Read<T> withBucket(String bucket) {
      Preconditions.checkArgumentNotNull(bucket);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              connectionConfiguration().uri(), connectionConfiguration().database(), bucket);
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public <X> Read<X> withParser(Parser<X> parser) {
      Preconditions.checkArgumentNotNull(parser);
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
      builder.addIfNotNull(DisplayData.item("uri", connectionConfiguration().uri()));
      builder.addIfNotNull(DisplayData.item("database", connectionConfiguration().database()));
      builder.addIfNotNull(DisplayData.item("bucket", connectionConfiguration().bucket()));
      builder.addIfNotNull(
          DisplayData.item("parser", parser() == null ? "null" : parser().getClass().getName()));
      builder.addIfNotNull(
          DisplayData.item("coder", coder() == null ? "null" : coder().getClass().getName()));
      builder.addIfNotNull(DisplayData.item("skew", skew()));
      builder.addIfNotNull(DisplayData.item("filter", filter()));
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      final BoundedGridFSSource source = new BoundedGridFSSource(this, null);
      org.apache.beam.sdk.io.Read.Bounded<ObjectId> objectIds =
          org.apache.beam.sdk.io.Read.from(source);
      PCollection<T> output =
          input
              .getPipeline()
              .apply(objectIds)
              .apply(
                  ParDo.of(
                      new DoFn<ObjectId, T>() {
                        @Nullable MongoClient mongo;
                        @Nullable GridFS gridfs;

                        @Setup
                        public void setup() {
                          mongo = source.spec.connectionConfiguration().setupMongo();
                          gridfs = source.spec.connectionConfiguration().setupGridFS(mongo);
                        }

                        @Teardown
                        public void teardown() {
                          if (mongo != null) {
                            mongo.close();
                            mongo = null;
                          }
                        }

                        @ProcessElement
                        public void processElement(final ProcessContext c) throws IOException {
                          Preconditions.checkStateNotNull(gridfs);
                          ObjectId oid = c.element();
                          GridFSDBFile file = gridfs.find(oid);
                          Parser<T> parser = Preconditions.checkStateNotNull(parser());
                          parser.parse(
                              file,
                              new ParserCallback<T>() {
                                @Override
                                public void output(T output, Instant timestamp) {
                                  Preconditions.checkArgumentNotNull(timestamp);
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
                          if (skew() != null) {
                            return skew();
                          } else {
                            return Duration.ZERO;
                          }
                        }
                      }));
      if (coder() != null) {
        output.setCoder(coder());
      }
      return output;
    }

    /** A {@link BoundedSource} for MongoDB GridFS. */
    protected static class BoundedGridFSSource extends BoundedSource<ObjectId> {

      private Read<?> spec;

      private @Nullable List<ObjectId> objectIds;

      BoundedGridFSSource(Read<?> spec, @Nullable List<ObjectId> objectIds) {
        this.spec = spec;
        this.objectIds = objectIds;
      }

      private DBCursor createCursor(GridFS gridfs) {
        if (spec.filter() != null) {
          DBObject query = (DBObject) JSON.parse(spec.filter());
          return gridfs.getFileList(query);
        }
        return gridfs.getFileList();
      }

      @Override
      public List<? extends BoundedSource<ObjectId>> split(
          long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        MongoClient mongo = spec.connectionConfiguration().setupMongo();
        try {
          GridFS gridfs = spec.connectionConfiguration().setupGridFS(mongo);
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
        try (MongoClient mongo = spec.connectionConfiguration().setupMongo();
            DBCursor cursor = createCursor(spec.connectionConfiguration().setupGridFS(mongo))) {
          long size = 0;
          while (cursor.hasNext()) {
            GridFSDBFile file = (GridFSDBFile) cursor.next();
            size += file.getLength();
          }
          return size;
        }
      }

      @Override
      public BoundedSource.BoundedReader<ObjectId> createReader(PipelineOptions options)
          throws IOException {
        return new GridFSReader(this, objectIds);
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        spec.populateDisplayData(builder);
      }

      @Override
      public Coder<ObjectId> getOutputCoder() {
        return SerializableCoder.of(ObjectId.class);
      }

      static class GridFSReader extends BoundedSource.BoundedReader<ObjectId> {
        final BoundedGridFSSource source;

        /* When split into bundles, this records the ObjectId's of the files for
         * this bundle.  Otherwise, this is null.  When null, a DBCursor of the
         * files is used directly to avoid having the ObjectId's queried and
         * loaded ahead of time saving time and memory.
         */
        final @Nullable List<ObjectId> objects;

        @Nullable MongoClient mongo;
        @Nullable DBCursor cursor;
        @Nullable Iterator<ObjectId> iterator;
        @Nullable ObjectId current;

        GridFSReader(BoundedGridFSSource source, @Nullable List<ObjectId> objects) {
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
            mongo = source.spec.connectionConfiguration().setupMongo();
            GridFS gridfs = source.spec.connectionConfiguration().setupGridFS(mongo);
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

        @Override
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

  /** Function that is called to write the data to the give GridFS OutputStream. */
  public interface WriteFn<T> extends Serializable {
    /**
     * Output the object to the given OutputStream.
     *
     * @param output The data to output
     * @param outStream The OutputStream
     */
    void write(T output, OutputStream outStream) throws IOException;
  }

  /** A {@link PTransform} to write data to MongoDB GridFS. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Pure
    abstract ConnectionConfiguration connectionConfiguration();

    @Pure
    abstract @Nullable Long chunkSize();

    @Pure
    abstract WriteFn<T> writeFn();

    @Pure
    abstract @Nullable String filename();

    @Pure
    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration connection);

      abstract Builder<T> setFilename(String filename);

      abstract Builder<T> setChunkSize(Long chunkSize);

      abstract Builder<T> setWriteFn(WriteFn<T> fn);

      abstract Write<T> build();
    }

    public Write<T> withUri(String uri) {
      Preconditions.checkArgumentNotNull(uri);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              uri, connectionConfiguration().database(), connectionConfiguration().bucket());
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public Write<T> withDatabase(String database) {
      Preconditions.checkArgumentNotNull(database);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              connectionConfiguration().uri(), database, connectionConfiguration().bucket());
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public Write<T> withBucket(String bucket) {
      Preconditions.checkArgumentNotNull(bucket);
      ConnectionConfiguration config =
          ConnectionConfiguration.create(
              connectionConfiguration().uri(), connectionConfiguration().database(), bucket);
      return toBuilder().setConnectionConfiguration(config).build();
    }

    public Write<T> withFilename(String filename) {
      Preconditions.checkArgumentNotNull(filename);
      return toBuilder().setFilename(filename).build();
    }

    public Write<T> withChunkSize(Long chunkSize) {
      Preconditions.checkArgumentNotNull(chunkSize);
      checkArgument(chunkSize > 1, "Chunk Size must be greater than 1", chunkSize);
      return toBuilder().setChunkSize(chunkSize).build();
    }

    public void validate(T input) {
      Preconditions.checkArgumentNotNull(filename(), "filename");
      Preconditions.checkArgumentNotNull(writeFn(), "writeFn");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("uri", connectionConfiguration().uri()));
      builder.addIfNotNull(DisplayData.item("database", connectionConfiguration().database()));
      builder.addIfNotNull(DisplayData.item("bucket", connectionConfiguration().bucket()));
      builder.addIfNotNull(DisplayData.item("chunkSize", chunkSize()));
      builder.addIfNotNull(DisplayData.item("filename", filename()));
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new GridFsWriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }
  }

  private static class GridFsWriteFn<T> extends DoFn<T, Void> {

    private final Write<T> spec;

    private transient @Nullable MongoClient mongo;
    private transient @Nullable GridFS gridfs;

    private transient @Nullable GridFSInputFile gridFsFile;
    private transient @Nullable OutputStream outputStream;

    public GridFsWriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      mongo = spec.connectionConfiguration().setupMongo();
      gridfs = spec.connectionConfiguration().setupGridFS(mongo);
    }

    @StartBundle
    public void startBundle() {
      GridFS gridfs = Preconditions.checkStateNotNull(this.gridfs);
      String filename = Preconditions.checkStateNotNull(spec.filename());
      GridFSInputFile gridFsFile = gridfs.createFile(filename);
      if (spec.chunkSize() != null) {
        gridFsFile.setChunkSize(spec.chunkSize());
      }
      outputStream = gridFsFile.getOutputStream();

      this.gridFsFile = gridFsFile;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      Preconditions.checkStateNotNull(outputStream);
      T record = context.element();
      spec.writeFn().write(record, outputStream);
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (outputStream != null) {
        OutputStream outputStream = this.outputStream;
        outputStream.flush();
        outputStream.close();
        this.outputStream = null;
      }
      if (gridFsFile != null) {
        gridFsFile = null;
      }
    }

    @Teardown
    public void teardown() throws Exception {
      try {
        if (outputStream != null) {
          OutputStream outputStream = this.outputStream;
          outputStream.flush();
          outputStream.close();
          this.outputStream = null;
        }
        if (gridFsFile != null) {
          gridFsFile = null;
        }
      } finally {
        if (mongo != null) {
          mongo.close();
          mongo = null;
          gridfs = null;
        }
      }
    }
  }
}
