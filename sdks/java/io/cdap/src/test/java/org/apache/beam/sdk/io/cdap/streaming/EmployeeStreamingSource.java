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
package org.apache.beam.sdk.io.cdap.streaming;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import java.io.IOException;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.io.cdap.EmployeeConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/** Imitation of CDAP {@link StreamingSource} plugin. Used to test {@link CdapIO#read()}. */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(EmployeeStreamingSource.NAME)
@Description("Plugin reads Employee in streaming")
public class EmployeeStreamingSource extends StreamingSource<StructuredRecord> {

  public static final String NAME = "EmployeeStreamingSource";

  private final EmployeeConfig config;

  public EmployeeStreamingSource(EmployeeConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector); // validate when macros are not substituted
    collector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext)
      throws IOException {
    FailureCollector collector = streamingContext.getFailureCollector();
    config.validate(collector); // validate when macros are substituted
    collector.getOrThrowException();

    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    return jssc.receiverStream(new EmployeeReceiver(config))
        .map(jsonString -> transform(jsonString, config));
  }

  public static StructuredRecord transform(String value, EmployeeConfig config) {
    StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
    builder.set("id", value);
    builder.set("name", "Employee " + value);
    return builder.build();
  }
}
