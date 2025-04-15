import XCTest
@testable import NyaruDB2

struct TestDocument: Codable, Equatable {
    let id: Int
    var name: String
    let created_at: String
}

final class NDBCollectionCRUDTests: XCTestCase {
    
    var db: NyaruDB2!
    var testCollection: NDBCollection!
    var tempPath: String!
    
    override func setUp() async throws {
        tempPath = NSTemporaryDirectory().appending(UUID().uuidString)
        db = try NyaruDB2(path: tempPath, shardKey: nil, compressionMethod: .none, fileProtectionType: .none)
        testCollection = NDBCollection(db: db, name: "TestCollection", indexes: ["id"], partitionKey: "created_at")
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(atPath: tempPath)
    }
    
    func testCRUDOperations() async throws {
        // 1. Insert
        let doc1 = TestDocument(id: 1, name: "Alice", created_at: "2022-01-01")
        let doc2 = TestDocument(id: 2, name: "Bob", created_at: "2022-01-01")
        
        try await testCollection.insert(doc1)
        try await testCollection.insert(doc2)
        
        var fetched: [TestDocument] = try await testCollection.fetch()
        XCTAssertEqual(fetched.count, 2, "Após inserção, deve haver 2 documentos")
        
        // 2. Read/FindOne
        let query: [String: Any] = ["id": 1]
        let found: TestDocument? = try await testCollection.findOne(query: query, shardKey: "created_at", shardValue: "2022-01-01")
        XCTAssertNotNil(found, "findOne deve retornar um documento")
        XCTAssertEqual(found?.name, "Alice", "O documento encontrado deve ter nome 'Alice'")
        
        // 3. Update
        // Atualiza doc1 de "Alice" para "Alicia"
        var updatedDoc1 = doc1
        updatedDoc1.name = "Alicia"
        try await testCollection.update(updatedDoc1, matching: { (doc: TestDocument) in doc.id == 1 })
        
        fetched = try await testCollection.fetch()
        let updated = fetched.first { $0.id == 1 }
        XCTAssertEqual(updated?.name, "Alicia", "O nome do documento atualizado deve ser 'Alicia'")
        
        // 4. Delete
        // Remove o documento com id == 2 (Bob)
        try await testCollection.delete(where: { (doc: TestDocument) in doc.id == 2 })
        
        fetched = try await testCollection.fetch()
        XCTAssertEqual(fetched.count, 1, "Após deleção, deve sobrar apenas 1 documento")
        XCTAssertEqual(fetched.first?.id, 1, "O documento restante deve ter id 1")
    }
}
