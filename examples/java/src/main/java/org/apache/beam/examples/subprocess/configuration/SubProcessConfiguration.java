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
package org.apache.beam.examples.subprocess.configuration;

import java.io.Serializable;

/**
 * Configuration file used to setup the Process kernel for execution of the external library Values
 * are copied from the Options to all them to be Serializable.
 */
@SuppressWarnings({"serial", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
public class SubProcessConfiguration implements Serializable {

  // Source GCS directory where the C++ library is located gs://bucket/tests
  public String sourcePath;

  // Working directory for the process I/O
  public String workerPath;

  // The maximum time to wait for the sub-process to complete
  public Integer waitTime;

  // "As sub-processes can be heavy weight match the concurrency level to num cores on the machines"
  public Integer concurrency;

  // Should log files only be uploaded if error
  public Boolean onlyUpLoadLogsOnError;

  public Boolean getOnlyUpLoadLogsOnError() {
    return onlyUpLoadLogsOnError;
  }

  public void setOnlyUpLoadLogsOnError(Boolean onlyUpLoadLogsOnError) {
    this.onlyUpLoadLogsOnError = onlyUpLoadLogsOnError;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }

  public String getWorkerPath() {
    return workerPath;
  }

  public void setWorkerPath(String workerPath) {
    this.workerPath = workerPath;
  }

  public Integer getWaitTime() {
    return waitTime;
  }

  public void setWaitTime(Integer waitTime) {
    this.waitTime = waitTime;
  }

  public Integer getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(Integer concurrency) {
    this.concurrency = concurrency;
  }
}
