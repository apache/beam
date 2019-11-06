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
package org.elasticsearch.bootstrap;

import java.util.function.Consumer;

/**
 * We need a real Elasticsearch instance to properly test the IO (split, slice API, scroll API,
 * ...). Starting at ES 5, to have Elasticsearch embedded, we are forced to use Elasticsearch test
 * framework. But this framework checks for class duplicates in classpath and it cannot be
 * deactivated. When the class duplication come from a dependency, then it cannot be avoided.
 * Elasticsearch community does not provide a way of deactivating the jar hell test, so skip it by
 * making this hack. In this case duplicate class is class:
 * org.apache.maven.surefire.report.SafeThrowable jar1: surefire-api-2.20.jar jar2:
 * surefire-junit47-2.20.jar
 */
class JarHell {

  @SuppressWarnings("EmptyMethod")
  public static void checkJarHell(Consumer<String> output) {}
}
