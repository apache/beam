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

/// FieldType is essentially a richer form of Coder and fortunately shares a lot of the same encoding so we can reuse it
public indirect enum FieldType {
    case unspecified

    case byte, int16, int32, int64, float, double, string, datetime, boolean, bytes
    case decimal(Int, Int)

    case logical(String, Schema)
    case row(Schema)

    case nullable(FieldType)
    case array(FieldType)
    case repeated(FieldType)
    case map(FieldType, FieldType)

    public var baseType: FieldType {
        switch self {
        case let .nullable(baseType):
            baseType
        case let .array(baseType):
            baseType
        case let .repeated(baseType):
            baseType
        default:
            self
        }
    }
}
