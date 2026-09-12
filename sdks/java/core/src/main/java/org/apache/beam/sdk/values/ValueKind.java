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
package org.apache.beam.sdk.values;

/** The type of change operation represented by a Change Data Capture (CDC) record. */
public enum ValueKind {
  /** Indicates a new record was created in the source system. */
  INSERT,

  /**
   * Indicates the state of a record immediately <b>before</b> an update occurred. This is typically
   * used to identify the previous values of modified columns or to locate the record via its
   * primary key.
   */
  UPDATE_BEFORE,

  /**
   * Indicates the state of a record immediately <b>after</b> an update occurred. Represents the
   * current, valid state of the record following the change.
   */
  UPDATE_AFTER,

  /** Indicates that an existing record was removed from the source system. */
  DELETE
}
