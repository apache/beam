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
package org.apache.beam.runners.jobsubmission;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Message;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * Contains common code for writing and reading portable pipeline jars.
 *
 * <p>Jar layout:
 *
 * <ul>
 *   <li>META-INF/
 *       <ul>
 *         <li>MANIFEST.MF
 *       </ul>
 *   <li>BEAM-PIPELINE/
 *       <ul>
 *         <li>pipeline-manifest.json
 *         <li>[1st pipeline (default)]
 *             <ul>
 *               <li>pipeline.json
 *               <li>pipeline-options.json
 *               <li>artifacts/
 *                   <ul>
 *                     <li>...artifact files...
 *                   </ul>
 *             </ul>
 *         <li>[nth pipeline]
 *             <ul>
 *               Same as above
 *         </ul>
 *   </ul>
 *   <li>...Java classes...
 * </ul>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class PortablePipelineJarUtils {
  private static final String ARTIFACT_FOLDER = "artifacts";
  private static final String PIPELINE_FOLDER = "BEAM-PIPELINE";
  private static final String PIPELINE = "pipeline.json";
  private static final String PIPELINE_OPTIONS = "pipeline-options.json";
  private static final String PIPELINE_MANIFEST = PIPELINE_FOLDER + "/pipeline-manifest.json";

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper(new JsonFactory().configure(Feature.AUTO_CLOSE_TARGET, false));

  private static class PipelineManifest {
    public String defaultJobName;
  }

  private static InputStream getResourceFromClassPath(String resourcePath) throws IOException {
    InputStream inputStream =
        PortablePipelineJarUtils.class.getClassLoader().getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new FileNotFoundException(
          String.format("Resource %s not found on classpath.", resourcePath));
    }
    return inputStream;
  }

  /** Populates {@code builder} using the JSON resource specified by {@code resourcePath}. */
  private static void parseJsonResource(String resourcePath, Message.Builder builder)
      throws IOException {
    try (InputStream inputStream = getResourceFromClassPath(resourcePath)) {
      String contents = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(contents, builder);
    }
  }

  public static Pipeline getPipelineFromClasspath(String jobName) throws IOException {
    Pipeline.Builder builder = Pipeline.newBuilder();
    parseJsonResource(getPipelineUri(jobName), builder);
    return builder.build();
  }

  public static Struct getPipelineOptionsFromClasspath(String jobName) throws IOException {
    Struct.Builder builder = Struct.newBuilder();
    parseJsonResource(getPipelineOptionsUri(jobName), builder);
    return builder.build();
  }

  static String getPipelineUri(String jobName) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + PIPELINE;
  }

  static String getPipelineOptionsUri(String jobName) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + PIPELINE_OPTIONS;
  }

  static String getArtifactUri(String jobName, String artifactId) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + ARTIFACT_FOLDER + "/" + artifactId;
  }

  public static String getDefaultJobName() throws IOException {
    try (InputStream inputStream = getResourceFromClassPath(PIPELINE_MANIFEST)) {
      PipelineManifest pipelineManifest =
          OBJECT_MAPPER.readValue(inputStream, PipelineManifest.class);
      return pipelineManifest.defaultJobName;
    }
  }

  public static void writeDefaultJobName(JarOutputStream outputStream, String jobName)
      throws IOException {
    outputStream.putNextEntry(new JarEntry(PIPELINE_MANIFEST));
    PipelineManifest pipelineManifest = new PipelineManifest();
    pipelineManifest.defaultJobName = jobName;
    OBJECT_MAPPER.writeValue(outputStream, pipelineManifest);
  }
}
