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
public extension String {
    static func beamUrn(_ name:String,type:String="beam",version:String="v1") -> String {
        return "beam:\(type):\(name):\(version)"
    }

    static func coderUrn(_ name:String,version:String="v1") -> String {
        return .beamUrn(name,type:"coder",version:version)
    }

    static func runnerUrn(_ name:String,version:String="v1") -> String {
        return .beamUrn(name,type:"runner",version:version)
    }

    static func doFnUrn(_ name:String,sdk:String="swiftsdk",version:String="v1") -> String {
        return .beamUrn(name,type:"dofn:\(sdk)",version:version)
    }

    static func transformUrn(_ name:String,version:String="v1") -> String {
        return .beamUrn(name,type:"transform",version:version)
    }
}
