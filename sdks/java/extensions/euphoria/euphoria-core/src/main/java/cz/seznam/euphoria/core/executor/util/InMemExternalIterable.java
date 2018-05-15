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
package cz.seznam.euphoria.core.executor.util;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.ExternalIterable;
import java.util.Iterator;

/**
 * An {@code ExternalIterable}, that is stored in memory. Use this class with care, because it might
 * cause OOME or other performance issues.
 */
@Audience(Audience.Type.INTERNAL)
public class InMemExternalIterable<T> implements ExternalIterable<T> {

  private final Iterable<T> wrap;

  public InMemExternalIterable(Iterable<T> wrap) {
    this.wrap = wrap;
  }

  @Override
  public Iterator<T> iterator() {
    return wrap.iterator();
  }

  @Override
  public void close() {
    // nop
  }
}
