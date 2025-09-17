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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillStateUtilTest {

  @Test
  public void testEncodeKey() {
    StateNamespaceForTest namespace = new StateNamespaceForTest("key");
    StateTag<SetState<Integer>> foo = StateTags.set("foo", VarIntCoder.of());
    ByteString bytes = WindmillStateUtil.encodeKey(namespace, foo);
    assertEquals("key+ufoo", bytes.toStringUtf8());
  }

  @Test
  public void testEncodeKeyNested() {
    // Hypothetical case where a namespace/tag encoding depends on a call to encodeKey
    // This tests if thread locals in WindmillStateUtil are not reused with nesting
    StateNamespaceForTest namespace1 = new StateNamespaceForTest("key");
    StateTag<SetState<Integer>> tag1 = StateTags.set("foo", VarIntCoder.of());
    StateTag<SetState<Integer>> tag2 =
        new StateTag<SetState<Integer>>() {
          @Override
          public void appendTo(Appendable sb) throws IOException {
            WindmillStateUtil.encodeKey(namespace1, tag1);
            sb.append("tag2");
          }

          @Override
          public String getId() {
            return "";
          }

          @Override
          public StateSpec<SetState<Integer>> getSpec() {
            return null;
          }

          @Override
          public SetState<Integer> bind(StateBinder binder) {
            return null;
          }
        };

    StateNamespace namespace2 =
        new StateNamespaceForTest("key") {
          @Override
          public void appendTo(Appendable sb) throws IOException {
            WindmillStateUtil.encodeKey(namespace1, tag1);
            sb.append("namespace2");
          }
        };
    ByteString bytes = WindmillStateUtil.encodeKey(namespace2, tag2);
    assertEquals("namespace2+tag2", bytes.toStringUtf8());
  }
}
