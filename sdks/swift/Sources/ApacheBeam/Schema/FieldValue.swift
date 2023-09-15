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

/// Dynamic representation of a Row that lets us treat a row value like JSON. Obviously this is less performant and
/// when using schema objects internally, particularly in PCollections we would favor @Row structs
@dynamicMemberLookup
public indirect enum FieldValue {
    // Variable width numbers
    case int(Int, FieldType)
    case float(Double, FieldType)
    case decimal(Decimal, FieldType)

    // Other scalar types
    case boolean(Bool)
    case string(String)
    case datetime(Date)
    case bytes(Data)
    case null
    case undefined

    case logical(String, FieldValue)
    case row(Schema, [FieldValue])
    case array([FieldValue])
    case repeated([FieldValue])
    case map([(FieldValue, FieldValue)])

    var isNull: Bool {
        switch self {
        case .null, .undefined:
            true
        default:
            false
        }
    }

    var isUndefined: Bool {
        switch self {
        case .undefined:
            true
        default:
            false
        }
    }

    // Similar to baseValue in BeamValue just extracts
    // the value from a scalar value. Use wisely
    var baseValue: Any? {
        switch self {
        case let .int(value, _):
            value
        case let .float(value, _):
            value
        case let .datetime(value):
            value
        case let .string(value):
            value
        default:
            nil
        }
    }

    var intValue: Int? {
        if case let .int(value, _) = self {
            return value
        }
        return nil
    }

    var doubleValue: Double? {
        if case let .float(value, _) = self {
            return value
        }
        return nil
    }

    var dateValue: Date? {
        if case let .datetime(value) = self {
            return value
        }
        return nil
    }

    var stringValue: String? {
        if case let .string(value) = self {
            return value
        }
        return nil
    }

    subscript(index: Int) -> FieldValue? {
        if case let .row(_, values) = self {
            return values[index]
        } else if case let .array(values) = self {
            return values[index]
        } else if case let .repeated(values) = self {
            return values[index]
        }
        return nil
    }

    subscript(key: String) -> FieldValue? {
        get {
            if case let .map(entries) = self {
                for entry in entries {
                    if key == entry.0.stringValue {
                        return entry.1
                    }
                }
            }
            return nil
        }
        set {
            if case var .map(entries) = self {
                if let value = newValue {
                    if let ndx = entries.firstIndex(where: { $0.0.stringValue == key }) {
                        entries[ndx] = (entries[ndx].0, value)
                    } else {
                        entries.append((.string(key), value))
                    }
                } else {
                    entries.removeAll(where: { $0.0.stringValue == key })
                }
            }
        }
    }

    subscript(dynamicMember member: String) -> FieldValue? {
        get {
            if case let .row(schema, array) = self {
                if let index = schema.fields.firstIndex(where: { $0.name == member }) {
                    return array[index]
                }
            }
            return nil
        }
        set {
            if case .row(let schema, var array) = self {
                if let index = schema.fields.firstIndex(where: { $0.name == member }) {
                    array[index] = newValue ?? .null
                }
            }
        }
    }
}

public extension FieldValue {
    init(_ schema: Schema, _ content: (inout FieldValue) -> Void = { _ in }) {
        var output: FieldValue = .row(schema, (0 ..< schema.fields.count).map { _ in .undefined })
        content(&output)
        self = output
    }
}
