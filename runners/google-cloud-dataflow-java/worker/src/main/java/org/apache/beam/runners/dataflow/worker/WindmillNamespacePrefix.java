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

import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * A prefix for a Windmill state or timer tag to separate user state and timers from system state
 * and timers.
 */
enum WindmillNamespacePrefix {
  USER_NAMESPACE_PREFIX {
    @Override
    ByteString byteString() {
      return USER_NAMESPACE_BYTESTRING;
    }
  },

  SYSTEM_NAMESPACE_PREFIX {
    @Override
    ByteString byteString() {
      return SYSTEM_NAMESPACE_BYTESTRING;
    }
  };

  abstract ByteString byteString();

  private static final ByteString USER_NAMESPACE_BYTESTRING = ByteString.copyFromUtf8("/u");
  private static final ByteString SYSTEM_NAMESPACE_BYTESTRING = ByteString.copyFromUtf8("/s");
}
