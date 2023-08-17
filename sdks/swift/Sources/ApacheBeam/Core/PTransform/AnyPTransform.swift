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

public struct AnyPTransform : _PrimitivePTransform {
    let type: Any.Type
    var transform: Any
    
    let expandClosure: (Any) -> AnyPTransform
    let expansionType: Any.Type
    
    
    public init<T>(_ transform: T) where T: PTransform {
        if let anyTransform = transform as? AnyPTransform {
            self = anyTransform
        } else {
            self.type = T.self
            self.expansionType = T.Expansion.self
            self.transform = transform
            self.expandClosure = { AnyPTransform(($0 as! T).expand) }
        }
    }
}

extension AnyPTransform : ParentPTransform {
    public var children: [AnyPTransform] {
        (transform as? ParentPTransform)?.children ?? []
    }
}
