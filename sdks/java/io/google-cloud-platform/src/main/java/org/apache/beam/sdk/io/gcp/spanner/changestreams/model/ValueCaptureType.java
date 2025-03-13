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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * Represents the capture type of a change stream. The only supported value at the moment is
 * OLD_AND_NEW_VALUES, meaning that {@link Mod}s will include the column values before and after the
 * database operations were applied.
 */
@DefaultCoder(AvroCoder.class)
public enum ValueCaptureType {
  NEW_ROW,
  NEW_VALUES,
  OLD_AND_NEW_VALUES,
  NEW_ROW_AND_OLD_VALUES,
  UNKNOWN
}
