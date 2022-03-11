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
package org.apache.beam.sdk.io.cdap.hubspot.source.batch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import java.io.IOException;
import org.apache.beam.sdk.io.cdap.hubspot.common.HubspotPagesIterator;
import org.apache.beam.sdk.io.cdap.hubspot.common.SourceHubspotConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** RecordReader implementation, which reads object instances from Hubspot. */
public class HubspotRecordReader extends RecordReader<NullWritable, JsonElement> {

  protected static final Gson GSON = new GsonBuilder().create();

  private JsonElement currentObject;
  private HubspotPagesIterator hubspotPagesIterator;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(HubspotInputFormatProvider.PROPERTY_CONFIG_JSON);
    SourceHubspotConfig sourceHubspotConfig = GSON.fromJson(configJson, SourceHubspotConfig.class);
    hubspotPagesIterator = new HubspotPagesIterator(sourceHubspotConfig);
  }

  @Override
  public boolean nextKeyValue() {
    if (!hubspotPagesIterator.hasNext()) {
      return false;
    }

    currentObject = hubspotPagesIterator.next();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public JsonElement getCurrentValue() throws IOException, InterruptedException {
    return currentObject;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
