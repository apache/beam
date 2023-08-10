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

public indirect enum Coder {
    // Special
    case unknown(String)
    case custom(Data)
    
    // Scalar standard coders
    case double,varint,fixedint,byte,bytes,string,boolean,globalwindow
    
    // Composite standard coders
    
    case keyvalue(Coder,Coder)
    case iterable(Coder)
    case lengthprefix(Coder)
    case windowedvalue(Coder,Coder)
    
    // TODO: Row Coder
}


public extension Coder {
    
    var urn: String {
        switch self {
        case let .unknown(name):
            return .coderUrn(name)
        case .custom(_):
            return .coderUrn("custom")
        case .double:
            return .coderUrn("double")
        case .varint:
            return .coderUrn("varint")
        case .fixedint:
            return .coderUrn("integer")
        case .bytes:
            return .coderUrn("bytes")
        case .byte:
            return .coderUrn("byte")
        case .string:
            return .coderUrn("string_utf8")
        case .boolean:
            return .coderUrn("bool")
        case .globalwindow:
            return .coderUrn("global_window")
        case .keyvalue(_, _):
            return .coderUrn("kv")
        case .iterable(_):
            return .coderUrn("iterable")
        case .lengthprefix(_):
            return .coderUrn("length_prefix")
        case .windowedvalue(_, _):
            return .coderUrn("windowed_value")
        }
    }
    
    static let capabilities:[String] = ["byte","bytes","bool","varint","double","integer","string_utf8","length_prefix","kv","iterable","windowed_value","global_window"]
        .map({ .coderUrn($0) })
}

extension Coder : Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.urn)
        switch self {
        case let .keyvalue(k,v):
            k.hash(into:&hasher)
            v.hash(into:&hasher)
        case let .iterable(c):
            c.hash(into:&hasher)
        case let .lengthprefix(c):
            c.hash(into:&hasher)
        case let .windowedvalue(v, w):
            v.hash(into:&hasher)
            w.hash(into:&hasher)
        default:
            break
        }
    }
}

extension Coder : Equatable {
    public static func ==(_ lhs: Coder,_ rhs: Coder) -> Bool {
        switch (lhs,rhs) {
        case let (.unknown(a),.unknown(b)):
            return a == b
        case let (.custom(a),.custom(b)):
            return a == b
        case (.double,.double):
            return true
        case (.varint,.varint):
            return true
        case (.fixedint,.fixedint):
            return true
        case (.bytes,.bytes):
            return true
        case (.byte,.byte):
            return true
        case (.string,.string):
            return true
        case (.boolean,.boolean):
            return true
        case (.globalwindow,.globalwindow):
            return true
        case let (.keyvalue(lk,lv),.keyvalue(rk, rv)):
            return lk == rk && lv == rv
        case let (.iterable(a),.iterable(b)):
            return a == b
        case let (.lengthprefix(a),.lengthprefix(b)):
            return a == b
        case let (.windowedvalue(lv, lw),.windowedvalue(rv,rw)):
            return lv == rv && lw == rw
        default:
            return false
        }
    }
}



protocol CoderContainer {
    subscript(name:String) -> CoderProto? { get }
}

struct PipelineCoderContainer : CoderContainer {
    let pipeline: PipelineProto
    subscript(name: String) -> CoderProto? {
        pipeline.components.coders[name]
    }
}

struct BundleCoderContainer : CoderContainer {
    let bundle : ProcessBundleDescriptorProto
    subscript(name: String) -> CoderProto? {
        bundle.coders[name]
    }
}

extension Coder {
    static func of(name:String,in container:CoderContainer) -> Coder? {
        if let baseCoder = container[name] {
            switch baseCoder.spec.urn {
            case "beam:coder:bytes:v1":
                return .bytes
            case "beam:coder:varint:v1":
                return .varint
            case "beam:coder:string_utf8:v1":
                return .string
            case "beam:coder:double:v1":
                return .double
            case "beam:coder:iterable:v1":
                return .iterable(.of(name: baseCoder.componentCoderIds[0], in: container)!)
            case "beam:coder:kv:v1":
                return .keyvalue(
                    .of(name: baseCoder.componentCoderIds[0],in:container)!,
                    .of(name: baseCoder.componentCoderIds[1], in:container)!)
            case "beam:coder:global_window:v1":
                return .globalwindow
            case "beam:coder:windowed_value:v1":
                return .windowedvalue(
                    .of(name: baseCoder.componentCoderIds[0],in:container)!,
                    .of(name: baseCoder.componentCoderIds[1],in:container)!)
            case "beam:coder:length_prefix:v1":
                return .lengthprefix(.of(name: baseCoder.componentCoderIds[0],in:container)!)
            default:
                return .unknown(baseCoder.spec.urn)
            }
        } else {
            return nil
        }
    }
}


