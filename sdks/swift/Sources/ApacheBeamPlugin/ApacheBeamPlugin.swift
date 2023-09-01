
// Macros for use with the Apache Beam Project
#if canImport(SwiftCompilerPlugin)
import SwiftCompilerPlugin
import SwiftSyntaxMacros


@main
struct ApacheBeamPlugin : CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        FnRegistrationMacro.self
    ]
}

#endif
