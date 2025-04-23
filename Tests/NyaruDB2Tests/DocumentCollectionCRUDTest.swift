//
//  DocumentCollectionCRUDTest.swift
//  NyaruDB2
//
//  Created by demetrius albuquerque on 11/04/25.
//

import XCTest
@testable import NyaruDB2

struct TestDocument: Codable, Equatable {
    let id: Int
    var name: String
    let created_at: String
}

final class DocumentCollectionCRUDTests: XCTestCase {
    
    var storage: StorageEngine!
    var statsEngine: StatsEngine!
    var testCollection: DocumentCollection!
    var tempPath: String!
    
    override func setUp() async throws {
        // Create a unique temporary directory for the test.
        tempPath = NSTemporaryDirectory().appending(UUID().uuidString)
        let tempURL = URL(fileURLWithPath: tempPath)
        try FileManager.default.createDirectory(at: tempURL, withIntermediateDirectories: true)
        
        // Initialize the StorageEngine.
        storage = try StorageEngine(
            path: tempPath,
            compressionMethod: .none,
            fileProtectionType: .none
        )

        statsEngine = StatsEngine(storage: storage)

        
        // Instantiate the collection using the storage instance.
        testCollection = DocumentCollection(
            storage: storage,
            statsEngine: statsEngine,
            name: "TestCollection",
            indexes: ["id"],
            partitionKey: "created_at"
        )
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(atPath: tempPath)
    }
    
    func testCRUDOperations() async throws {
        // 1. Insert two documents.
        let doc1 = TestDocument(id: 1, name: "Alice", created_at: "2022-01-01")
        let doc2 = TestDocument(id: 2, name: "Bob", created_at: "2022-01-01")
        
        try await testCollection.insert(doc1)
        try await testCollection.insert(doc2)
        
        var fetched: [TestDocument] = try await testCollection.fetch()
        XCTAssertEqual(fetched.count, 2, "After insertion, there should be 2 documents")
        
        // 2. Read: findOne using a directed query.
        let query: [String: Any] = ["id": 1]
        let found: TestDocument? = try await testCollection.findOne(query: query, shardKey: "created_at", shardValue: "2022-01-01")
        XCTAssertNotNil(found, "findOne should return a document")
        XCTAssertEqual(found?.name, "Alice", "The found document should have the name 'Alice'")
        
        // 3. Update: change doc1's name from 'Alice' to 'Alicia'
        var updatedDoc1 = doc1
        updatedDoc1.name = "Alicia"
        try await testCollection.update(updatedDoc1, matching: { (doc: TestDocument) in doc.id == 1 })
        
        fetched = try await testCollection.fetch()
        let updated = fetched.first { $0.id == 1 }
        XCTAssertEqual(updated?.name, "Alicia", "The updated document's name should be 'Alicia'")
        
        // 4. Delete: remove the document with id == 2 (Bob)
        try await testCollection.delete { (doc: TestDocument) in doc.id == 2 }
        
        fetched = try await testCollection.fetch()
        XCTAssertEqual(fetched.count, 1, "After deletion, there should be only 1 document left")
        XCTAssertEqual(fetched.first?.id, 1, "The remaining document should have id 1")
    }
}
