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

/**
 * Table schema for HCatalog.
 *
 * <p><b>WARNING:</b>This package requires users to declare their own dependency on
 * org.apache.hive:hive-exec and org.apache.hive.hcatalog. At the time of this Beam release every
 * released version of those packages had a transitive dependency on a version of log4j vulnerable
 * to CVE-2021-44228. We strongly encourage users to pin a non-vulnerable version of log4j when
 * using this package. See <a href="https://github.com/apache/beam/issues/21426">Issue #21426</a>.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;
