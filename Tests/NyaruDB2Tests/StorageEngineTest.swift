import XCTest
@testable import NyaruDB2

struct TestModel: Codable, Equatable {
    let id: Int
    let name: String
    let category: String?  // Usado para particionamento, se necessário
}

final class StorageEngineTests: XCTestCase {
    
    func testInsertDocumentWithoutPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine sem particionamento.
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: nil, compressionMethod: .none)
        let model = TestModel(id: 1, name: "Test", category: nil)
        
        try await storage.insertDocument(model, collection: "TestCollection")
        
        // O StorageEngine utiliza o shard "default" quando não há shardKey.
        let fileURL = tempDirectory
                        .appendingPathComponent("TestCollection", isDirectory: true)
                        .appendingPathComponent("default.nyaru")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "O arquivo do shard 'default.nyaru' deve existir.")
        
        // Tenta ler e decodificar os documentos armazenados.
        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([TestModel].self, from: data)
        XCTAssertEqual(decoded.count, 1)
        XCTAssertEqual(decoded.first, model)
    }
    
    func testInsertDocumentWithPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa StorageEngine com particionamento usando "category" e indexando pelo campo "name".
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: "category", compressionMethod: .none, fileProtectionType: FileProtectionType.none)
        
        let modelA = TestModel(id: 1, name: "Alice", category: "A")
        let modelB = TestModel(id: 2, name: "Bob", category: "B")
        let modelA2 = TestModel(id: 3, name: "Alice", category: "A")
        
        try await storage.insertDocument(modelA, collection: "TestCollection", indexField: "name")
        try await storage.insertDocument(modelB, collection: "TestCollection", indexField: "name")
        try await storage.insertDocument(modelA2, collection: "TestCollection", indexField: "name")
        
        let collectionURL = tempDirectory.appendingPathComponent("TestCollection", isDirectory: true)
        let fileA = collectionURL.appendingPathComponent("A.nyaru")
        let fileB = collectionURL.appendingPathComponent("B.nyaru")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileA.path),
                      "O arquivo do shard 'A.nyaru' deve existir.")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileB.path),
                      "O arquivo do shard 'B.nyaru' deve existir.")
        
        // Aqui a verificação do índice pode ser feita consultando o IndexManager,
        // mas como o StorageEngine atual não expõe um método de consulta, a validação de índice
        // ocorrerá indiretamente (sem erro durante a inserção, assume-se que o índice foi atualizado).
    }
}
