//
//  NyaruDB2Test.swift
//  NyaruDB2
//
//  Created by demetrius albuquerque on 11/04/25.
//

import XCTest

@testable import NyaruDB2

final class NyaruDB2Tests: XCTestCase {
    func testExample() throws {
        let db = try NyaruDB2()
        XCTAssertNotNil(db.storage)
    }
}
