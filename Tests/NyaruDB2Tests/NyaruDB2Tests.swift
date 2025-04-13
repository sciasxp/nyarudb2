import XCTest

@testable import NyaruDB2

final class NyaruDB2Tests: XCTestCase {
    func testExample() throws {
        // Teste básico
        let db = try NyaruDB2()
        XCTAssertNotNil(db.storage)
    }
}
