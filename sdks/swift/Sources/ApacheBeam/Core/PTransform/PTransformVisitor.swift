//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/19/23.
//

import Foundation

public protocol PTransformVisitor {
    func visit<T:PTransform>(_ transform: T)
}

public extension PTransform {
    func _visitChildren<V:PTransformVisitor>(_ visitor: V) {
        visitor.visit(expand)
    }
}

public typealias PTransformVisitorFn<V:PTransformVisitor> = (V) -> ()

protocol PTransformReducer {
    associatedtype Result
    
    static func reduce<T: PTransform>(into partialResult: inout Result,nextTransform: T)
    static func reduce<T: PTransform>(partialResult: Result, nextTransform: T) -> Result
}

extension PTransformReducer {
    static func reduce<T: PTransform>(into partialResult: inout Result,nextTransform: T) {
        partialResult = Self.reduce(partialResult: partialResult, nextTransform: nextTransform)
    }
    static func reduce<T: PTransform>(partialResult: Result, nextTransform: T) -> Result {
        var result = partialResult
        Self.reduce(into: &result, nextTransform: nextTransform)
        return result
    }
}

final class ReducerVisitor<R: PTransformReducer> : PTransformVisitor {
    var result: R.Result
    init(initialResult: R.Result) {
        result = initialResult
    }
    
    func visit<T>(_ transform: T) where T : PTransform {
        R.reduce(into: &result, nextTransform: transform)
    }
}

extension PTransformReducer {
    typealias Visitor = ReducerVisitor<Self>
}

