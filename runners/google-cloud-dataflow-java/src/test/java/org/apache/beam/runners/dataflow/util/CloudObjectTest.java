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
package org.apache.beam.runners.dataflow.util;

import com.google.common.testing.EqualsTester;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudObjectTest {

  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(CloudObject.forFloat(1.0), CloudObject.forFloat(1.0))
        .addEqualityGroup(CloudObject.forInteger(3L), CloudObject.forInteger(3L))
        .addEqualityGroup(CloudObject.forFloat(3.0))
        .addEqualityGroup(CloudObject.forString("foo"), CloudObject.forString("foo"))
        .addEqualityGroup(CloudObject.forClassName("foo.Bar"), CloudObject.forClassName("foo.Bar"))
        .addEqualityGroup(
            CloudObject.fromSpec(ImmutableMap.of(PropertyNames.OBJECT_TYPE_NAME, "ValuesDoFn")),
            CloudObject.fromSpec(ImmutableMap.of(PropertyNames.OBJECT_TYPE_NAME, "ValuesDoFn")))
        .testEquals();
  }
}
