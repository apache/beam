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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;

public class MongoDbGridFSIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbGridFSIO.class);

  /** Read data from GridFS. */
  public static Read read() {
    return new Read(new Read.GridFSOptions(null, null, null));
  }



  static class Read extends PTransform<PBegin, PCollection<String>> {
    public Read withUri(String uri) {
      return new Read(new GridFSOptions(uri, options.database, options.bucket));
    }

    public Read withDatabase(String database) {
      return new Read(new GridFSOptions(options.uri, database, options.bucket));
    }

    public Read withBucket(String bucket) {
      return new Read(new GridFSOptions(options.uri, options.database, bucket));
    }

    GridFSOptions options;

    Read(GridFSOptions options) {
      this.options = options;
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      PCollection<String> output = input.apply(Create.of(options))
          .apply(ParDo.of(new ReadFn()));

      return output;
    }

    /**
     * A {@link DoFn} executing the query to read files from GridFS.
     */
    public static class ReadFn extends DoFn<GridFSOptions, String> {

      private ReadFn() {
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        GridFSOptions options = context.element();
        Mongo mongo = options.uri == null ? new Mongo() : new Mongo(new MongoURI(options.uri));
        DB db = options.database == null ? mongo.getDB("gridfs") : mongo.getDB(options.database);
        GridFS gridfs = options.bucket == null ? new GridFS(db) : new GridFS(db, options.bucket);
        for (GridFSDBFile file : gridfs.find(new BasicDBObject())) {
          Instant ts = new Instant(file.getUploadDate().getTime());
          InputStream ins = file.getInputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
          String line;
          while ((line = reader.readLine()) != null) {
            context.outputWithTimestamp(line, ts);
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
