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
package org.apache.beam.sdk.util;

import java.io.Serializable;

/**
 * A wrapper object storing a Python function definition that can be evaluated to Python callables
 * in Python SDK.
 *
 * <p>The snippet of Python code can be a valid Python expression (such as {@code lambda x: x * x}
 * or {str.upper}), a fully qualified name (such as {@code math.sin}), or a complete, multi-line
 * function or class definition (such as {@code def foo(x): ...} or {@code class Foo: ...}).
 *
 * <p>Any lines preceding the function definition are first evaluated to provide context in which to
 * define the function which can be useful to declare imports or any other needed values, e.g.
 *
 * <pre>
 * import math
 *
 * def helper(x):
 *   return x * x
 *
 * def func(y):
 *   return helper(y) + y
 * </pre>
 *
 * in which case {@code func} would get applied to each element.
 */
public class PythonCallableSource implements Serializable {
  private final String pythonCallableCode;

  private PythonCallableSource(String pythonCallableCode) {
    this.pythonCallableCode = pythonCallableCode;
  }

  public static PythonCallableSource of(String pythonCallableCode) {
    // TODO(https://github.com/apache/beam/issues/21568): check syntactic correctness of Python code
    // if possible
    return new PythonCallableSource(pythonCallableCode);
  }

  public String getPythonCallableCode() {
    return pythonCallableCode;
  }
}
