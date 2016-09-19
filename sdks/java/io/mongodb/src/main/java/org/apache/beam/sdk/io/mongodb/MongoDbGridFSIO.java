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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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
 * pipeline.apply(MongoDbIO.read()
 *   .withUri("mongodb://localhost:27017")
 *   .withDatabase("my-database")
 *   .withBucket("my-bucket"))
 *
 * }</pre>
 */
public class MongoDbGridFSIO {
  static class InputStreamToStrings implements SerializableFunction<InputStream, Iterable<String>> {
    @Override
    public Iterable<String> apply(InputStream input) {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        try {
          final String initialValue = reader.readLine();
          return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                    return new Iterator<String>() {
                        String val = initialValue;
                        @Override
                        public boolean hasNext() {
                            return val != null;
                        }

                        @Override
                        public String next() {
                            String valRet = val;
                            try {
                                val = reader.readLine();
                            } catch (IOException e) {
                                val = null;
                            }
                            return valRet;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("read-only");
                        }
                    };
            }
          };
        } catch (IOException ex) {
            return null;
        }
    }
  };


  /** Read data from GridFS. */
  public static Read<String> read() {
    return new Read<String>(new Read.GridFSOptions(null, null, null),
                            new InputStreamToStrings());
  }

  static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    public Read<T> withUri(String uri) {
      return new Read<T>(new GridFSOptions(uri, options.database,
                                           options.bucket), transform);
    }

    public Read<T> withDatabase(String database) {
      return new Read<T>(new GridFSOptions(options.uri, database,
                                           options.bucket), transform);
    }

    public Read<T> withBucket(String bucket) {
      return new Read<T>(new GridFSOptions(options.uri, options.database, bucket),
                         transform);
    }

    public Read<T> withParsingFn(SerializableFunction<InputStream, Iterable<T>> f) {
      return new Read<T>(options, f);
    }

    private final GridFSOptions options;
    private final SerializableFunction<InputStream, Iterable<T>> transform;


    Read(GridFSOptions options,
         SerializableFunction<InputStream, Iterable<T>> transform) {
      this.options = options;
      this.transform = transform;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      PCollection<T> output = input.apply(Create.of(options))
          .apply(ParDo.of(new ReadFn(transform)));
      return output;
    }

    /**
     * A {@link DoFn} executing the query to read files from GridFS.
     */
    public static class ReadFn<T> extends DoFn<GridFSOptions, T> {
      private final SerializableFunction<InputStream, Iterable<T>> transform;

      private ReadFn(SerializableFunction<InputStream, Iterable<T>> transform) {
          this.transform = transform;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        GridFSOptions options = context.element();
        Mongo mongo = options.uri == null ? new Mongo() : new Mongo(new MongoURI(options.uri));
        DB db = options.database == null ? mongo.getDB("gridfs") : mongo.getDB(options.database);
        GridFS gridfs = options.bucket == null ? new GridFS(db) : new GridFS(db, options.bucket);
        for (GridFSDBFile file : gridfs.find(new BasicDBObject())) {
          Instant ts = new Instant(file.getUploadDate().getTime());
          for (T t : transform.apply(file.getInputStream())) {
              context.outputWithTimestamp(t, ts);
          }
        }
      }
    }

    static class GridFSOptions implements Serializable {
      private final String uri;
      private final String database;
      private final String bucket;

      GridFSOptions(String uri, String database, String bucket) {
        this.uri = uri;
        this.database = database;
        this.bucket = bucket;
      }
    }

  }

}
