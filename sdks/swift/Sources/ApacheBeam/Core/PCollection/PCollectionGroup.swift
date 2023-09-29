//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/21/23.
//

import Foundation

@dynamicMemberLookup
public protocol PCollectionGroup {
    subscript(dynamicMember member:String) -> AnyPCollection? { get }
    subscript<Of>(of:Of.Type) -> PCollection<Of>? { get }
    subscript(named: String) -> AnyPCollection? { get throws }
    subscript<Of>(named: String,of:Of.Type) -> PCollection<Of>? { get }
}
