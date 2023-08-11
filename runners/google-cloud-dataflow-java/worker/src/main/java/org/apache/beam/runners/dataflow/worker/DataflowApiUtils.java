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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;

/** A utility class for generic interactions with the Google Cloud Dataflow API. */
public final class DataflowApiUtils {
  /**
   * Determines the serialized size (in bytes) of the {@link GenericJson} object that will be
   * serialized and sent to the Google Cloud Dataflow service API.
   *
   * <p>Uses only constant memory.
   */
  public static long computeSerializedSizeBytes(GenericJson object) throws IOException {
    JsonFactory factory = object.getFactory();
    if (factory == null) {
      factory = Transport.getJsonFactory();
    }

    CountingOutputStream stream = new CountingOutputStream(ByteStreams.nullOutputStream());
    JsonGenerator generator = null;
    try {
      generator = factory.createJsonGenerator(stream, StandardCharsets.UTF_8);
      generator.serialize(object);
      generator.close(); // also closes the stream.
    } finally {
      if (generator != null) {
        generator.close();
      }
    }
    return stream.getCount();
  }

  // Prevent construction of utility class.
  private DataflowApiUtils() {}
}
