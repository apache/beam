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

public protocol PTransformVisitor {
    func visit<T: PTransform>(_ transform: T)
}

public extension PTransform {
    func _visitChildren(_ visitor: some PTransformVisitor) {
        visitor.visit(expand)
    }
}

public typealias PTransformVisitorFn<V: PTransformVisitor> = (V) -> Void

protocol PTransformReducer {
    associatedtype Result

    static func reduce<T: PTransform>(into partialResult: inout Result, nextTransform: T)
    static func reduce<T: PTransform>(partialResult: Result, nextTransform: T) -> Result
}

extension PTransformReducer {
    static func reduce(into partialResult: inout Result, nextTransform: some PTransform) {
        partialResult = reduce(partialResult: partialResult, nextTransform: nextTransform)
    }

    static func reduce(partialResult: Result, nextTransform: some PTransform) -> Result {
        var result = partialResult
        Self.reduce(into: &result, nextTransform: nextTransform)
        return result
    }
}

final class ReducerVisitor<R: PTransformReducer>: PTransformVisitor {
    var result: R.Result
    init(initialResult: R.Result) {
        result = initialResult
    }

    func visit(_ transform: some PTransform) {
        R.reduce(into: &result, nextTransform: transform)
    }
}

extension PTransformReducer {
    typealias Visitor = ReducerVisitor<Self>
}
