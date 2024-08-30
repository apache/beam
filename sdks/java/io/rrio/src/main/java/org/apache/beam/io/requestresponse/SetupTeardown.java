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
package org.apache.beam.io.requestresponse;

import java.io.Serializable;

/**
 * Provided by user and called within {@link org.apache.beam.sdk.transforms.DoFn.Setup} and @{link
 * org.apache.beam.sdk.transforms.DoFn.Teardown} lifecycle methods of {@link Call}'s {@link
 * org.apache.beam.sdk.transforms.DoFn}.
 */
public interface SetupTeardown extends Serializable {

  /** Called during the {@link org.apache.beam.sdk.transforms.DoFn}'s setup lifecycle method. */
  void setup() throws UserCodeExecutionException;

  /** Called during the {@link org.apache.beam.sdk.transforms.DoFn}'s teardown lifecycle method. */
  void teardown() throws UserCodeExecutionException;
}
