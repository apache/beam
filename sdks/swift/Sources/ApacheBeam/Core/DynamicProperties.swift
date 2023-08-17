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

import Logging

public protocol DynamicProperty { }

@propertyWrapper
public struct PInput<Of> : DynamicProperty {
    public var wrappedValue: PCollectionStream<Of>
    
    public init(wrappedValue: PCollectionStream<Of> = .init()) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct POutput<Of> : DynamicProperty {
    public var wrappedValue: PCollectionStream<Of>
    public init(wrappedValue: PCollectionStream<Of> = .init()) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct Logger : DynamicProperty {
    public var wrappedValue: Logging.Logger
    public init(wrappedValue: Logging.Logger = Logging.Logger(label: "TEST")) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct Serialized<Value:Codable> : DynamicProperty {
    public var wrappedValue: Value?
    public init(wrappedValue: Value? = nil) {
        self.wrappedValue = wrappedValue
    }
}
