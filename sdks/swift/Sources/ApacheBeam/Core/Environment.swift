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

