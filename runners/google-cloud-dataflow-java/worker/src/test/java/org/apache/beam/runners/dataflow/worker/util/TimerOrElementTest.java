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
package org.apache.beam.runners.dataflow.worker.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.util.TimerOrElement.TimerOrElementCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TimerOrElement}. */
@RunWith(JUnit4.class)
public class TimerOrElementTest {
  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        TimerOrElementCoder.of(
            KvCoder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE)));
  }

  @Test
  public void testCoderCanBeDecodedFromCloudObject() {
    CloudObject cloudObject =
        CloudObject.forClassName(
            "com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder");
    List<CloudObject> component =
        Collections.singletonList(
            CloudObjects.asCloudObject(
                KvCoder.of(VarLongCoder.of(), ByteArrayCoder.of()), /*sdkComponents=*/ null));
    Structs.addList(cloudObject, PropertyNames.COMPONENT_ENCODINGS, component);

    Coder<?> decoded = CloudObjects.coderFromCloudObject(cloudObject);
    assertThat(decoded, instanceOf(TimerOrElementCoder.class));
    TimerOrElementCoder<?> decodedCoder = (TimerOrElementCoder<?>) decoded;
    assertThat(decodedCoder.getKeyCoder(), equalTo(VarLongCoder.of()));
    assertThat(decodedCoder.getElementCoder(), equalTo(ByteArrayCoder.of()));
  }
}
