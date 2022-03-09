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
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.schema.Schema;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cdap.zendesk.batch.util.ZendeskBatchSourceConstants;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Input format class which generates input splits for each given object and initializes appropriate
 * record reader.
 */
@SuppressWarnings("rawtypes")
public class ZendeskInputFormat extends InputFormat {

  private static final Gson GSON = new GsonBuilder().create();
  private static final Type OBJECTS_TYPE = new TypeToken<List<String>>() {}.getType();
  private static final Type SCHEMAS_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration configuration = context.getConfiguration();
    List<String> objects =
        GSON.fromJson(
            configuration.get(ZendeskBatchSourceConstants.PROPERTY_OBJECTS_JSON), OBJECTS_TYPE);
    ZendeskBatchSourceConfig config =
        GSON.fromJson(
            configuration.get(ZendeskBatchSourceConstants.PROPERTY_CONFIG_JSON),
            ZendeskBatchSourceConfig.class);
    Set<String> subdomains = config.getSubdomains();

    return subdomains.stream()
        .flatMap(subdomain -> objects.stream().map(object -> new ZendeskSplit(subdomain, object)))
        .collect(Collectors.toList());
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    ZendeskSplit multiSplit = (ZendeskSplit) split;
    String object = multiSplit.getObject();

    Configuration configuration = context.getConfiguration();
    Map<String, String> schemas =
        GSON.fromJson(
            configuration.get(ZendeskBatchSourceConstants.PROPERTY_SCHEMAS_JSON), SCHEMAS_TYPE);
    Schema schema = Schema.parseJson(schemas.get(object));
    ObjectType objectType = ObjectType.fromString(object);

    return new ZendeskRecordReader(multiSplit.getSubdomain(), objectType, schema);
  }
}
