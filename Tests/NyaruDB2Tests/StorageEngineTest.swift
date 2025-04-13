import XCTest
@testable import NyaruDB2

// Modelo para testes
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
        
        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([TestModel].self, from: data)
        XCTAssertEqual(decoded.count, 1)
        XCTAssertEqual(decoded.first, model)
    }
    
    func testInsertDocumentWithPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa StorageEngine com particionamento usando "category" e indexando pelo campo "name".
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: "category", compressionMethod: .none, fileProtectionType: .none)
        
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
    }
    
    // Teste para quando o documento NÃO possui a chave de particionamento
    func testInsertDocumentMissingPartitionKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine com particionamento usando "category".
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: "category", compressionMethod: .none, fileProtectionType: .none)
        
        // Cria um modelo sem a propriedade "category"
        struct ModelMissingPartition: Codable, Equatable {
            let id: Int
            let name: String
        }
        let model = ModelMissingPartition(id: 1, name: "NoCategory")
        
        // Como o documento não contém "category", deve lançar um erro
        do {
            try await storage.insertDocument(model, collection: "TestCollection")
            XCTFail("Expected error not thrown")
        } catch {
            guard case StorageEngine.StorageError.partitionKeyNotFound(let key) = error else {
                XCTFail("Erro inesperado: \(error)")
                return
            }
            XCTAssertEqual(key, "category")
        }
    }
    
    // Teste para quando o documento NÃO possui o campo de índice solicitado
    func testInsertDocumentMissingIndexKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa StorageEngine com particionamento usando "category".
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: "category", compressionMethod: .none, fileProtectionType: .none)
        
        // Modelo com chave de particionamento, mas sem a chave "name" (será usada para index)
        struct ModelMissingIndex: Codable, Equatable {
            let id: Int
            let category: String
        }
        let model = ModelMissingIndex(id: 1, category: "X")
        
        do {
            try await storage.insertDocument(model, collection: "TestCollection", indexField: "name")
            XCTFail("Expected error not thrown")
        } catch {
            guard case StorageEngine.StorageError.indexKeyNotFound(let key) = error else {
                XCTFail("Erro inesperado: \(error)")
                return
            }
            XCTAssertEqual(key, "name")
        }
    }
    
    // Teste de inserção múltipla e validação do conteúdo do shard
    func testMultipleInsertionsAndContent() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine sem particionamento
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: nil, compressionMethod: .none)
        
        let model1 = TestModel(id: 1, name: "Test1", category: nil)
        let model2 = TestModel(id: 2, name: "Test2", category: nil)
        let model3 = TestModel(id: 3, name: "Test3", category: nil)
        
        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")
        try await storage.insertDocument(model3, collection: "TestCollection")
        
        let fileURL = tempDirectory
                        .appendingPathComponent("TestCollection", isDirectory: true)
                        .appendingPathComponent("default.nyaru")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "O arquivo do shard 'default.nyaru' deve existir.")
        
        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([TestModel].self, from: data)
        XCTAssertEqual(decoded.count, 3)
        XCTAssertEqual(decoded, [model1, model2, model3])
    }
}
