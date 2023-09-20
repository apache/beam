//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/20/23.
//

import Foundation

public struct Group<Content> {
    let content: Content
    public init(@PTransformBuilder content: () -> Content) {
        self.content = content()
    }
}

extension Group: _PrimitivePTransform, PTransform where Content: PTransform {
    public func _visitChildren<V>(_ visitor: V) where V: PTransformVisitor {
        visitor.visit(content)
    }
}

extension Group: ParentPTransform where Content: PTransform {
    public var children: [AnyPTransform] { (content as? ParentPTransform)?.children ?? [AnyPTransform(content)] }
}

extension Group: GroupPTransform where Content: PTransform { }
