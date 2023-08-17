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

import GRPC

extension Server {
    var endpoint: ApiServiceDescriptor {
        get throws {
            let address = self.channel.localAddress!
            if let pathname = address.pathname {
                return ApiServiceDescriptor(unixAddress: pathname)
            }
            if let host = address.ipAddress,let port = address.port {
                return ApiServiceDescriptor(host: host, port: port)
            }
            throw ApacheBeamError.runtimeError("Can't determine endpoint for \(address)")
        }
    }
}

