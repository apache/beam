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
import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros

public struct FnRegistrationMacro: FreestandingMacro, ExpressionMacro {
    public static func expansion(of node: some SwiftSyntax.FreestandingMacroExpansionSyntax, in _: some SwiftSyntaxMacros.MacroExpansionContext) throws -> SwiftSyntax.ExprSyntax {
        guard let argument = node.argumentList.first?.expression else {
            fatalError("compiler bug: the macro doesn't have any arguments")
        }
        let id = UUID().uuidString
        return "(\(argument),\"\(raw: id)\")"
    }
}
