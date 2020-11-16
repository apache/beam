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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** BeamBuiltinFunctionClass interface. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class BeamBuiltinFunctionProvider {
  public Map<String, List<Method>> getBuiltinMethods() {
    List<Method> methods = Arrays.asList(getClass().getMethods());
    return methods.stream()
        .filter(BeamBuiltinFunctionProvider::isUDF)
        .collect(
            Collectors.groupingBy(method -> method.getDeclaredAnnotation(UDF.class).funcName()));
  }

  private static boolean isUDF(Method m) {
    return m.getDeclaredAnnotation(UDF.class) != null;
  }
}
