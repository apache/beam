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

import Foundation

/// The worker side realization of a PCollection that supports reading and writing
public final class PCollectionStream<Of> : AsyncSequence {
    public typealias Element = (Of,Date,Window)
    
    private let stream: AsyncStream<Element>
    private let emitter: AsyncStream<Element>.Continuation
    
    public init() {
        (self.stream,self.emitter) = AsyncStream.makeStream(of:Element.self)
    }
    
    public func makeAsyncIterator() -> AsyncStream<Element>.Iterator {
        return stream.makeAsyncIterator()
    }
    
    public func finish() {
        emitter.finish()
    }
    

    public func emit(_ value: Element) {
        emitter.yield(value)
    }
    
    public func emit(_ value: Of,timestamp: Date = .now,window:Window = .global) {
        emit((value,timestamp,window))
    }
    
    // Implementing key-value pair conversion is a little more complicated because we need
    // to convert to a KV<K,V> from what is essentially a KV<Any,Any> which requires us to
    // cast the key and the value first and then construct the KV from that. There might be
    // a more clever way of doing this, but I don't know what it is.
    
    func emit<K,V>(key: K,value: [V],timestamp: Date,window:Window) {
        emit(KV(key,values:value) as! Of,timestamp:timestamp,window:window)
    }
    
    func emit<K>(key: K,value: BeamValue,timestamp: Date,window:Window) throws {
        //We overload the key value type as both (K,[V]) and (K,V). It may be worth considering
        //having an explicit Pair type in addition to KV to simplify this decoding a little bit.
        //
        // On the other hand, the code is already written and pretty straightforward and there
        // won't be much in the way of new scalar values.
        if case .array(let array) = value {
            switch array.first {
            case .boolean(_):emit(key:key,value:array.map({$0.baseValue! as! Bool}),timestamp:timestamp,window:window)
            case .bytes(_):  emit(key:key,value:array.map({$0.baseValue! as! Data}),timestamp:timestamp,window:window)
            case .double(_): emit(key:key,value:array.map({$0.baseValue! as! Double}),timestamp:timestamp,window:window)
            case .integer(_):emit(key:key,value:array.map({$0.baseValue! as! Int}),timestamp:timestamp,window:window)
            case .string(_): emit(key:key,value:array.map({$0.baseValue! as! String}),timestamp:timestamp,window:window)
            default:
                throw ApacheBeamError.runtimeError("Can't use \(String(describing:array.first)) as a value in a key value pair")
            }
        } else {
            switch value {
            case let .boolean(v):emit(key:key,value:[v],timestamp:timestamp,window:window)
            case let .bytes(v):  emit(key:key,value:[v],timestamp:timestamp,window:window)
            case let .double(v): emit(key:key,value:[v],timestamp:timestamp,window:window)
            case let .integer(v):emit(key:key,value:[v],timestamp:timestamp,window:window)
            case let .string(v): emit(key:key,value:[v],timestamp:timestamp,window:window)
            default:
                throw ApacheBeamError.runtimeError("Can't use \(value) as a value in a key value pair")
            }

        }
    }
    
    // Unwrap all of the actual value types (not windows or windowed elements)
    func emit(_ value: BeamValue,timestamp: Date,window:Window) throws {
        if case let .kv(key,value) = value {
            // Unwrap the key first
            switch key {
            case let .boolean(v):try emit(key:v!,value:value,timestamp:timestamp,window:window)
            case let .bytes(v):  try emit(key:v!,value:value,timestamp:timestamp,window:window)
            case let .double(v): try emit(key:v!,value:value,timestamp:timestamp,window:window)
            case let .integer(v):try emit(key:v!,value:value,timestamp:timestamp,window:window)
            case let .string(v): try emit(key:v!,value:value,timestamp:timestamp,window:window)
            default:
                throw ApacheBeamError.runtimeError("Can't use \(value) as a value in a key value pair")
            }
        } else {
            emit(value.baseValue as! Of,timestamp: timestamp,window: window)
        }
    }
    
    /// Mostly intended as a convenience function for bundle processing this emit unwraps a windowed value
    /// for further conversion in (non-public) versions of the function.
    public func emit(_ value: BeamValue) throws {
        switch value {
        case let .windowed(value, timestamp, _, window):
            try emit(value,timestamp:timestamp,window:window.baseValue as! Window)
        default:
            throw ApacheBeamError.runtimeError("Only windowed values can be sent directly to a PCollectionStream, not \(value)")
        }
    }
    
}
