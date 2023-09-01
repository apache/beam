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

/// Dynamic representation of a Row that lets us treat a row value like JSON. Obviously this is less performant and
/// when using schema objects internally, particularly in PCollections we would favor @Row structs
@dynamicMemberLookup
public indirect enum FieldValue {
    // Variable width numbers
    case int(Int,FieldType)
    case float(Double,FieldType)
    case decimal(Decimal,FieldType)

    // Other scalar types
    case boolean(Bool)
    case string(String)
    case datetime(Date)
    case bytes(Data)
    case null
    case undefined
    
    case logical(String,FieldValue)
    case row(Schema,[FieldValue])
    case array([FieldValue])
    case repeated([FieldValue])
    case map([(FieldValue,FieldValue)])
    
    var isNull : Bool {
        switch self {
        case .null,.undefined:
            true
        default:
            false
        }
    }
    
    var isUndefined : Bool {
        switch self {
        case .undefined:
            true
        default:
            false
        }
    }
    
    

    var intValue : Int? {
        if case .int(let value,_) = self {
            return value
        }
        return nil
    }
    
    var doubleValue : Double? {
        if case .float(let value,_) = self {
            return value
        }
        return nil
    }
    
    var dateValue : Date? {
        if case .datetime(let value) = self {
            return value
        }
        return nil
    }

    
    var stringValue : String? {
        if case let .string(value) = self {
            return value
        }
        return nil
    }
    
    subscript(index: Int) -> FieldValue? {
        if case .row(_,let values) = self {
            return values[index]
        } else if case let .array(values) = self {
            return values[index]
        } else if case let .repeated(values) = self {
            return values[index]
        }
        return nil
    }
    
    subscript(key: String) -> FieldValue? {
        if case let .map(entries) = self {
            for entry in entries {
                if key == entry.0.stringValue {
                    return entry.1
                }
            }
        }
        return nil
    }
    
    subscript(dynamicMember member: String) -> FieldValue? {
        if case let .row(schema, array) = self {
            var index = 0
            for field in schema.fields {
                if field.name == member {
                    return array[index]
                }
                index += 1
            }
        }
        return nil
    }
    
}

public extension FieldValue {
    init() {
        self = .undefined
    }
    
    init(_ schema: Schema,_ content: (inout FieldValue) -> Void = { _ in }) {
        var output : FieldValue = .row(schema,(0..<schema.fields.count).map { _ in .undefined })
        content(&output)
        self = output
    }
}

