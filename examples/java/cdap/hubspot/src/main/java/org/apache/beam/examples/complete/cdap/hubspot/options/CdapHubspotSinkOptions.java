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
package org.apache.beam.examples.complete.cdap.hubspot.options;

import org.apache.beam.examples.complete.cdap.hubspot.TxtToCdapHubspot;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link CdapHubspotSinkOptions} interface provides the custom execution options passed by the
 * executor at the command-line for {@link TxtToCdapHubspot} example.
 */
public interface CdapHubspotSinkOptions extends CdapHubspotOptions {

  @Validation.Required
  @Description("Input .txt file path with Hubspot records.")
  String getInputTxtFilePath();

  void setInputTxtFilePath(String inputTxtFilePath);

  @Validation.Required
  @Description(
      "Locks directory path where locks will be stored."
          + "This parameter is needed for Hadoop External Synchronization"
          + "(mechanism for acquiring locks related to the write job).")
  String getLocksDirPath();

  void setLocksDirPath(String locksDirPath);
}
