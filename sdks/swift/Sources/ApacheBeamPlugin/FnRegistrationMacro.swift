import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros
import Foundation

public struct FnRegistrationMacro : FreestandingMacro, ExpressionMacro {
    public static func expansion(of node: some SwiftSyntax.FreestandingMacroExpansionSyntax, in context: some SwiftSyntaxMacros.MacroExpansionContext) throws -> SwiftSyntax.ExprSyntax {
        guard let argument = node.argumentList.first?.expression else {
            fatalError("compiler bug: the macro doesn't have any arguments")
        }
        let id = UUID().uuidString
        return "(\(argument),\"\(raw:id)\")"
    }
    
}
