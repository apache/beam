/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.timeseries;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Description;

/** Pipeline options that allow configuring GCP options. */
@Experimental
public interface GCPBigTableTimeSeriesOptions extends TimeSeriesOptions, DataflowPipelineOptions {

  @Description("BigTable Instance ID - Used for GCP I/O ")
  String getBigTableInstanceId();

  void setBigTableInstanceId(String bigTableInstanceId);

  @Description("BigTable Table ID - Used for GCP I/O ")
  String getBigTableTableId();

  void setBigTableTableId(String projectId);
}
