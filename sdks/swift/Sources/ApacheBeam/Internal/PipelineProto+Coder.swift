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

extension PipelineProto {
    
    mutating func coder(from: Coder) -> PipelineComponent {
        let componentCoders:[String] = switch from {
        case let .keyvalue(keyCoder, valueCoder):
            [coder(from:keyCoder).name,coder(from:valueCoder).name]
        case let .iterable(valueCoder):
            [coder(from:valueCoder).name]
        case let .lengthprefix(valueCoder):
            [coder(from:valueCoder).name]
        case let .windowedvalue(valueCoder, windowCoder):
            [coder(from:valueCoder).name,coder(from:windowCoder).name]
        default:
            []
        }
        return coder { _ in
                .with {
                    $0.spec = .with {
                        $0.urn = from.urn
                        if case .custom(let data) = from {
                            $0.payload = data
                        }
                    }
                    $0.componentCoderIds = componentCoders
                }
        }
    }
}
