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
package org.apache.beam.sdk.io.iceberg.maintenance;

/** Categorization of files managed during Iceberg table maintenance operations. */
public enum FileCategory {
  /** Physical data file containing user rows. */
  DATA,

  /** Row-level position delete file. */
  POSITION_DELETES,

  /** Row-level equality delete file. */
  EQUALITY_DELETES,

  /** Iceberg manifest file referencing data or delete files. */
  MANIFEST,

  /** Iceberg manifest list file referencing manifest files. */
  MANIFEST_LIST,

  /** Iceberg Puffin statistics file. */
  STATISTICS
}
