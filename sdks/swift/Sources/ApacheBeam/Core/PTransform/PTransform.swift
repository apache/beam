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

/// Represents a composite transform
public protocol PTransform {
    associatedtype Expansion: PTransform
    
    var expand: Expansion { get }
}

public extension Never {
    var expand: Never {
        fatalError()
    }
}

extension Never : PTransform { }

/// Represents PTransforms that can't be expanded further. When constructing the pipeline the expansion
/// happens until we hit this point
public protocol _PrimitivePTransform : PTransform where Expansion == Never { }
public extension _PrimitivePTransform {
    var expand: Never {
        neverExpand(String(reflecting: Self.self)) }
}


public protocol ParentPTransform {
    var children: [AnyPTransform] { get }
}

protocol GroupPTransform : ParentPTransform { }

public func neverExpand(_ type: String) -> Never {
    fatalError("\(type) is a primitive PTransform and cannot be expanded.")
}
