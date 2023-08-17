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
import NIOCore


/// Representation of the API Service Descriptors used to communicate with runners (and vice versa)
public struct ApiServiceDescriptor {
    
    public enum EncodedAs {
        case json,textproto
    }
    
    let url: String

    public init(host:String,port:Int) {
        self.url = "\(host):\(port)"
    }
    public init(unixAddress:String) {
        self.url = "unix://\(unixAddress)"
    }
}

extension ApiServiceDescriptor {
    init(proto: Org_Apache_Beam_Model_Pipeline_V1_ApiServiceDescriptor) {
        self.url = proto.url
    }
}

extension ApiServiceDescriptor : ProtoConversion {
    
    func populate(_ proto: inout Org_Apache_Beam_Model_Pipeline_V1_ApiServiceDescriptor) throws {
        proto.url = self.url
    }
}

extension ApiServiceDescriptor : Hashable {
    
}

public extension ApiServiceDescriptor {
    static func from(env:String,format:EncodedAs = .textproto) throws -> ApiServiceDescriptor {
        switch format {
        case .textproto:
            ApiServiceDescriptor(proto: try .init(textFormatString: env))
        case .json:
            ApiServiceDescriptor(proto: try .init(jsonString: env))
        }
    }
}

public extension GRPCChannelPool {
    static func with(endpoint:ApiServiceDescriptor, eventLoopGroup: EventLoopGroup) throws -> GRPCChannel {
        let url = endpoint.url
        //TODO: Transport Security configuration
        if(url.starts(with: "unix://")) {
            return try GRPCChannelPool.with(target: .unixDomainSocket(url.replacing("unix://",with:"")),
                                        transportSecurity: .plaintext,
                                        eventLoopGroup: eventLoopGroup)
        } else {
            if let lastNdx = url.lastIndex(of: ":") {
                guard lastNdx.utf16Offset(in: url) > 0 else {
                    throw ApacheBeamError.runtimeError("Service URL must be of the form host:port")
                }
                let host = String(url.prefix(upTo: lastNdx))
                let port = Int(url.suffix(from:url.index(lastNdx,offsetBy:1)))!
                return try GRPCChannelPool.with(target: .host(host, port: port),
                                                transportSecurity: .plaintext,
                                                eventLoopGroup: eventLoopGroup)
            } else {
                throw ApacheBeamError.runtimeError("Service URL must be of the form host:port")
            }
        }
    }
}
