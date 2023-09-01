import Foundation

#if canImport(ApacheBeamPlugin)

@freestanding(expression)
public macro fnregister<T>(_ value: T) -> (T,String) = #externalMacro(module: "ApacheBeamPlugin", type: "FnRegistrationMacro")

#endif
