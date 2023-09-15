//
//  SchemaTests.swift
//  
//
//  Created by Byron Ellis on 9/1/23.
//

import XCTest
@testable import ApacheBeam

final class SchemaTests: XCTestCase {

    override func setUpWithError() throws {
    }

    override func tearDownWithError() throws {
    }

    func testSchemaDefinition() throws {
        let schema = Schema { fields in
            fields
                .string("id")
                .datetime("timestamp")
                .string("description")
        }
    }


}
