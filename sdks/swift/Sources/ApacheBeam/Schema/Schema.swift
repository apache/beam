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

public struct Field {
    let name: String
    let description: String?
    let type: FieldType
    var options: [String: (FieldType, FieldValue)]
    let position: Int?
}

public class FieldsBuilder {
    var fields: [Field] = []
}

public struct Schema {
    let id: UUID?
    let fields: [Field]
    let options: [String: (FieldType, FieldValue)]

    public init(id: UUID? = UUID(), options: [String: (FieldType, FieldValue)] = [:], fields: [Field]) {
        self.id = id
        self.options = options
        self.fields = fields
    }

    public init(id: UUID? = UUID(), options: [String: (FieldType, FieldValue)] = [:], _ fn: (inout FieldsBuilder) -> Void) {
        var builder = FieldsBuilder()
        fn(&builder)
        self = .init(id: id, options: options, fields: builder.fields)
    }

    public func row(_ populator: (inout FieldValue) -> Void) -> FieldValue {
        FieldValue(self, populator)
    }
}

extension Schema: Equatable {
    public static func == (lhs: Schema, rhs: Schema) -> Bool {
        lhs.id == rhs.id
    }
}

/// Scalar value convience functions
public extension FieldsBuilder {
    @discardableResult
    func field(_ name: String, type: FieldType, description: String? = nil, options: [String: (FieldType, FieldValue)] = [:]) -> FieldsBuilder {
        fields.append(Field(name: name, description: description, type: type, options: options, position: fields.count))
        return self
    }

    // TODO: Make macros to autogenerate the convenience cases
    @discardableResult
    func byte(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.byte) : .byte, description: description, options: [:])
    }

    @discardableResult
    func int16(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.int16) : .int16, description: description, options: [:])
    }

    @discardableResult
    func int32(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.int32) : .int32, description: description, options: [:])
    }

    @discardableResult
    func int64(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.int64) : .int64, description: description, options: [:])
    }

    @discardableResult
    func float(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.float) : .float, description: description, options: [:])
    }

    @discardableResult
    func double(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.double) : .double, description: description, options: [:])
    }

    @discardableResult
    func string(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.string) : .string, description: description, options: [:])
    }

    @discardableResult
    func datetime(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.datetime) : .datetime, description: description, options: [:])
    }

    @discardableResult
    func boolean(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.boolean) : .boolean, description: description, options: [:])
    }

    @discardableResult
    func bytes(_ name: String, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.bytes) : .bytes, description: description, options: [:])
    }

    @discardableResult
    func decimal(_ name: String, _ precision: Int, _ scale: Int, description: String? = nil, nullable: Bool = false) -> FieldsBuilder {
        field(name, type: nullable ? .nullable(.decimal(precision, scale)) : .decimal(precision, scale), description: description, options: [:])
    }

    @discardableResult
    func row(_ name: String, description: String? = nil, _ fn: (inout FieldsBuilder) -> Void) -> FieldsBuilder {
        field(name, type: .row(Schema { fn(&$0) }), description: description, options: [:])
    }
}
