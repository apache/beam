//
//  ExpansionServiceTests.swift
//  
//
//  Created by Byron Ellis on 9/15/23.
//

import XCTest
@testable import ApacheBeam

final class ExpansionServiceTests: XCTestCase {

    override func setUpWithError() throws {
    }

    override func tearDownWithError() throws {
    }

    func testConnectExpansionService() async throws {
        let client = try ExpansionClient(endpoint: .init(host: "localhost", port: 8097))
        let transforms = try await client.transforms()
        for t in transforms {
            print("\(t)")
        }
    }


}
