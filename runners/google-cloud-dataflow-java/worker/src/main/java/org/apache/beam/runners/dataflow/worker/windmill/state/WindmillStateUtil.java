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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

class WindmillStateUtil {
  /** Encodes the given namespace and address as {@code &lt;namespace&gt;+&lt;address&gt;}. */
  @VisibleForTesting
  static ByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
    try {
      // Use ByteStringOutputStream rather than concatenation and String.format. We build these keys
      // a lot, and this leads to better performance results. See associated benchmarks.
      ByteStringOutputStream stream = new ByteStringOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);

      // stringKey starts and ends with a slash.  We separate it from the
      // StateTag ID by a '+' (which is guaranteed not to be in the stringKey) because the
      // ID comes from the user.
      namespace.appendTo(writer);
      writer.write('+');
      address.appendTo(writer);
      writer.flush();
      return stream.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
