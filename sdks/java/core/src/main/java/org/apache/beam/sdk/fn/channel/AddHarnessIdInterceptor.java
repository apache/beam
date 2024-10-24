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
package org.apache.beam.sdk.fn.channel;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata.Key;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.MetadataUtils;

/** A {@link ClientInterceptor} that attaches a provided SDK Harness ID to outgoing messages. */
public class AddHarnessIdInterceptor {
  private static final Key<String> ID_KEY = Key.of("worker_id", Metadata.ASCII_STRING_MARSHALLER);

  public static ClientInterceptor create(String harnessId) {
    checkArgument(harnessId != null, "harnessId must not be null");
    Metadata md = new Metadata();
    md.put(ID_KEY, harnessId);
    return MetadataUtils.newAttachHeadersInterceptor(md);
  }

  // This is implemented via MetadataUtils, so we never actually create an instance of this class
  private AddHarnessIdInterceptor() {}
}
