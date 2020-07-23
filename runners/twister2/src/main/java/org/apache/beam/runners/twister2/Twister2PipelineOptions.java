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
package org.apache.beam.runners.twister2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/** Twister2PipelineOptions. */
public interface Twister2PipelineOptions extends PipelineOptions, StreamingOptions {

  @Description("set parallelism for Twister2 processor")
  @Default.Integer(1)
  int getParallelism();

  void setParallelism(int parallelism);

  @Description("Twister2 TSetEnvironment")
  @JsonIgnore
  TSetEnvironment getTSetEnvironment();

  void setTSetEnvironment(TSetEnvironment environment);

  @Description("Twister2 cluster type, supported types: standalone, nomad, kubernetes, mesos")
  @Default.String("standalone")
  String getClusterType();

  void setClusterType(String name);

  @Description(
      "Jar-Files to send to all workers and put on the classpath. The default value is all files from the classpath.")
  List<String> getFilesToStage();

  void setFilesToStage(List<String> value);

  @Description("Job file zip")
  String getJobFileZip();

  void setJobFileZip(String pathToZip);

  @Description("Job type, jar or java_zip")
  @Default.String("java_zip")
  String getJobType();

  void setJobType(String jobType);

  @Description("Twister2 home directory")
  String getTwister2Home();

  void setTwister2Home(String twister2Home);

  @Description("CPU's per worker")
  @Default.Integer(2)
  int getWorkerCPUs();

  void setWorkerCPUs(int workerCPUs);

  @Description("RAM allocated per worker")
  @Default.Integer(2048)
  int getRamMegaBytes();

  void setRamMegaBytes(int ramMegaBytes);
}
