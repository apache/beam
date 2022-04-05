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
package org.apache.beam.sdk.io.cdap.zendesk.batch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.io.cdap.zendesk.batch.http.CommentsPagedIterator;
import org.apache.beam.sdk.io.cdap.zendesk.batch.http.PagedIterator;
import org.apache.beam.sdk.io.cdap.zendesk.batch.util.ZendeskBatchSourceConstants;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** RecordReader implementation, which reads object from Zendesk. */
public class ZendeskRecordReader extends RecordReader<NullWritable, StructuredRecord> {

  private static final Gson GSON = new GsonBuilder().create();

  private final String subdomain;
  private final ObjectType objectType;
  private final Schema schema;

  private Iterator<String> pagedIterator;

  /**
   * Constructor for ZendeskRecordReader.
   *
   * @param subdomain the subdomain name
   * @param objectType the object type for which data to be fetched
   * @param schema the schema for the object
   */
  public ZendeskRecordReader(String subdomain, ObjectType objectType, Schema schema) {
    this.subdomain = subdomain;
    this.objectType = objectType;
    this.schema = schema;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(ZendeskBatchSourceConstants.PROPERTY_CONFIG_JSON);
    ZendeskBatchSourceConfig config = GSON.fromJson(configJson, ZendeskBatchSourceConfig.class);
    pagedIterator = createIterator(config);
  }

  @Override
  public boolean nextKeyValue() {
    return pagedIterator.hasNext();
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    String next = pagedIterator.next();
    return StructuredRecordStringConverter.fromJsonString(next, schema);
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (pagedIterator != null) {
      ((Closeable) pagedIterator).close();
    }
  }

  private Iterator<String> createIterator(ZendeskBatchSourceConfig config) {
    if (objectType == ObjectType.ARTICLE_COMMENTS || objectType == ObjectType.POST_COMMENTS) {
      return new CommentsPagedIterator(
          new PagedIterator(config, ObjectType.USERS_SIMPLE, subdomain),
          config,
          objectType,
          subdomain);
    }
    if (objectType == ObjectType.REQUESTS_COMMENTS) {
      return new CommentsPagedIterator(
          new PagedIterator(config, ObjectType.REQUESTS, subdomain), config, objectType, subdomain);
    }
    return new PagedIterator(config, objectType, subdomain);
  }
}
