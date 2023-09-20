//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/20/23.
//

import Foundation

@propertyWrapper
public struct PValue<Value> {
    public var wrappedValue: Value
    
    public init(wrappedValue: Value,name: String? = nil) {
        self.wrappedValue = wrappedValue
    }
}
