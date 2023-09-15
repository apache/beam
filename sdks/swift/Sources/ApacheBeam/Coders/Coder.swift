/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 *  License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an  AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

public indirect enum Coder {
    /// Catch-all for coders we don't understand. Mostly used for error reporting
    case unknown(String)
    // TODO: Actually implement this
    case custom(Data)

    /// Standard scalar coders. Does not necessarily correspond 1:1 with BeamValue. For example, varint and fixedint both map to integer
    case double, varint, fixedint, byte, bytes, string, boolean, globalwindow

    /// Composite coders.
    case keyvalue(Coder, Coder)
    case iterable(Coder)
    case lengthprefix(Coder)
    case windowedvalue(Coder, Coder)

    /// Schema-valued things
    case row(Schema)
}

public extension Coder {
    var urn: String {
        switch self {
        case let .unknown(name):
            .coderUrn(name)
        case .custom:
            .coderUrn("custom")
        case .double:
            .coderUrn("double")
        case .varint:
            .coderUrn("varint")
        case .fixedint:
            .coderUrn("integer")
        case .bytes:
            .coderUrn("bytes")
        case .byte:
            .coderUrn("byte")
        case .string:
            .coderUrn("string_utf8")
        case .boolean:
            .coderUrn("bool")
        case .globalwindow:
            .coderUrn("global_window")
        case .keyvalue:
            .coderUrn("kv")
        case .iterable:
            .coderUrn("iterable")
        case .lengthprefix:
            .coderUrn("length_prefix")
        case .windowedvalue:
            .coderUrn("windowed_value")
        case .row:
            .coderUrn("row")
        }
    }

    /// Static list of coders for use in capabilities arrays in environments.
    static let capabilities: [String] = ["byte", "bytes", "bool", "varint", "double", "integer", "string_utf8", "length_prefix", "kv", "iterable", "windowed_value", "global_window"]
        .map { .coderUrn($0) }
}

extension Coder: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(urn)
        switch self {
        case let .keyvalue(k, v):
            k.hash(into: &hasher)
            v.hash(into: &hasher)
        case let .iterable(c):
            c.hash(into: &hasher)
        case let .lengthprefix(c):
            c.hash(into: &hasher)
        case let .windowedvalue(v, w):
            v.hash(into: &hasher)
            w.hash(into: &hasher)
        default:
            break
        }
    }
}

extension Coder: Equatable {
    public static func == (_ lhs: Coder, _ rhs: Coder) -> Bool {
        switch (lhs, rhs) {
        case let (.unknown(a), .unknown(b)):
            a == b
        case let (.custom(a), .custom(b)):
            a == b
        case (.double, .double):
            true
        case (.varint, .varint):
            true
        case (.fixedint, .fixedint):
            true
        case (.bytes, .bytes):
            true
        case (.byte, .byte):
            true
        case (.string, .string):
            true
        case (.boolean, .boolean):
            true
        case (.globalwindow, .globalwindow):
            true
        case let (.row(ls), .row(rs)):
            ls == rs
        case let (.keyvalue(lk, lv), .keyvalue(rk, rv)):
            lk == rk && lv == rv
        case let (.iterable(a), .iterable(b)):
            a == b
        case let (.lengthprefix(a), .lengthprefix(b)):
            a == b
        case let (.windowedvalue(lv, lw), .windowedvalue(rv, rw)):
            lv == rv && lw == rw
        default:
            false
        }
    }
}

protocol CoderContainer {
    subscript(_: String) -> CoderProto? { get }
}

struct PipelineCoderContainer: CoderContainer {
    let pipeline: PipelineProto
    subscript(name: String) -> CoderProto? {
        pipeline.components.coders[name]
    }
}

struct BundleCoderContainer: CoderContainer {
    let bundle: ProcessBundleDescriptorProto
    subscript(name: String) -> CoderProto? {
        bundle.coders[name]
    }
}

extension Coder {
    static func of(name: String, in container: CoderContainer) throws -> Coder {
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
                return try .iterable(.of(name: baseCoder.componentCoderIds[0], in: container))
            case "beam:coder:kv:v1":
                return try .keyvalue(
                    .of(name: baseCoder.componentCoderIds[0], in: container),
                    .of(name: baseCoder.componentCoderIds[1], in: container)
                )
            case "beam:coder:global_window:v1":
                return .globalwindow
            case "beam:coder:windowed_value:v1":
                return try .windowedvalue(
                    .of(name: baseCoder.componentCoderIds[0], in: container),
                    .of(name: baseCoder.componentCoderIds[1], in: container)
                )
            case "beam:coder:length_prefix:v1":
                return try .lengthprefix(.of(name: baseCoder.componentCoderIds[0], in: container))
            case "beam:coder:row:v1":
                let proto: SchemaProto = try SchemaProto(serializedData: baseCoder.spec.payload)
                return .row(.from(proto))
            default:
                return .unknown(baseCoder.spec.urn)
            }
        } else {
            throw ApacheBeamError.runtimeError("Unable to location coder \(name) in container.")
        }
    }
}

public extension Coder {
    static func of<Of>(type _: Of?.Type) -> Coder? {
        .lengthprefix(.of(type: Of.self)!)
    }

    static func of<Of>(type _: [Of].Type) -> Coder? {
        .iterable(.of(type: Of.self)!)
    }

    static func of<Of>(type _: Of.Type) -> Coder? {
        // Beamables provider their own default coder implementation
        if let beamable = Of.self as? Beamable.Type {
            return beamable.coder
        }
        return nil
    }
}
