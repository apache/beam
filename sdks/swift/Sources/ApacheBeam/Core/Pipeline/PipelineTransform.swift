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

/// Enum for pipeline representable transforms as opposed to composite transforms
/// which are a user-side construct represented by PTransform
public enum PipelineTransform {
    case pardo(AnyPCollection, String, SerializableFn, [AnyPCollection])
    case impulse(AnyPCollection, AnyPCollection)
    case flatten([AnyPCollection], AnyPCollection)
    case groupByKey(AnyPCollection, AnyPCollection)
    case custom(AnyPCollection, String, Data, Environment?, [AnyPCollection])
    case composite(AnyPCollection, AnyPTransform)
    case external(AnyPCollection, String, FieldValue, [AnyPCollection])
}

extension PipelineTransform: PipelineMember {
    var roots: [PCollection<Never>] {
        switch self {
        case let .pardo(p, _, _, _):
            p.roots
        case let .impulse(p, _):
            p.roots
        case let .flatten(p, _):
            p.flatMap(\.roots)
        case let .groupByKey(p, _):
            p.roots
        case let .custom(p, _, _, _, _):
            p.roots
        case let .composite(p, _):
            p.roots
        case let .external(p, _, _, _):
            p.roots
        }
    }
}
