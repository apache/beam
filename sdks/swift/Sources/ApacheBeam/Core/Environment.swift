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


/// A struct that encodes the different types of available in Beam
public struct Environment {
    
    public enum Category {
        /// Default environment type. "default" is a reserved word so we use "system" here
        case system
        /// Process. command, arch, os, environment
        case process(String,String,String,[String:String])
        /// Docker container image
        case docker(String)
        /// External service using an api descriptor
        case external(ApiServiceDescriptor)
    }
    
    let category: Category
    let capabilities: [String]
    let dependencies: [ArtifactInfo]
    
    public init(_ category: Category = .system,capabilities:[String] = [],dependencies:[ArtifactInfo]) {
        self.category = category
        self.capabilities = capabilities
        self.dependencies = dependencies
    }
}


extension Environment : ProtoConversion {
    typealias Proto = EnvironmentProto
    
    var proto: EnvironmentProto {
        get throws {
            return try .with { p in
                p.urn = switch category {
                case .docker(_): .beamUrn("docker",type:"env")
                case .system: .beamUrn("default",type:"env")
                case .process(_, _, _, _): .beamUrn("process",type:"env")
                case .external(_): .beamUrn("external",type:"env")
                }
                p.capabilities = capabilities
                p.dependencies = dependencies.map({ $0.proto })
                
                if case let .docker(containerImage) = category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_DockerPayload.with {
                        $0.containerImage = containerImage
                    }.serializedData()
                }
                
                if case let .external(endpoint) = category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_ExternalPayload.with {
                        $0.endpoint = endpoint.proto
                    }.serializedData()
                }
                
                if case let .process(command,arch,os,env) = category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_ProcessPayload.with {
                        $0.arch = arch
                        $0.command = command
                        $0.os = os
                        $0.env = env
                    }.serializedData()
                }
            }
        }
    }
}
