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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;

/** Utility class to check size of BeamSQLRow iterable. */
public class CheckSize implements SerializableFunction<Iterable<Row>, Void> {
  private int size;

  public CheckSize(int size) {
    this.size = size;
  }

  @Override
  public Void apply(Iterable<Row> input) {
    int count = 0;
    for (Row row : input) {
      count++;
    }
    Assert.assertEquals(size, count);
    return null;
  }
}
