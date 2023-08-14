
import Foundation

/// Codable is already taken and besides Beamable is too good to pass up
public protocol Beamable {
    static var coder: Coder { get }
}

extension Data : Beamable {
    public static let coder:Coder = .bytes
}

extension String : Beamable {
    public static let coder:Coder = .string
}

extension Int : Beamable {
    public static let coder:Coder = .varint
}

extension Bool : Beamable {
    public static let coder:Coder = .boolean
}

