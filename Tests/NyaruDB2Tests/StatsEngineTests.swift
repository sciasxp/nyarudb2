import XCTest
@testable import NyaruDB2

struct TestModel: Codable, Equatable {
    let id: Int
    let name: String
    let category: String? // Usado para particionamento
}

final class StatsEngineTests: XCTestCase {

    func testGetCollectionStats() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine sem particionamento para simplificar
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: nil, compressionMethod: .none)
        // Insere 3 documentos em "StatsCollection"
        let models = [
            TestModel(id: 1, name: "Alice", category: nil),
            TestModel(id: 2, name: "Bob", category: nil),
            TestModel(id: 3, name: "Charlie", category: nil)
        ]
        for model in models {
            try await storage.insertDocument(model, collection: "StatsCollection")
        }
        
        // Crie o StatsEngine usando o storage
        let statsEngine = StatsEngine(storage: storage)
        // Obter estatísticas da coleção
        let stats = try await statsEngine.getCollectionStats("StatsCollection")
        
        XCTAssertEqual(stats.collectionName, "StatsCollection")
        XCTAssertEqual(stats.numberOfShards, 1, "Se não há particionamento, espera-se apenas o shard 'default'")
        XCTAssertEqual(stats.totalDocuments, 3, "A contagem total deve ser 3")
        XCTAssertTrue(stats.totalSizeInBytes > 0, "O tamanho total deve ser maior que zero")
        XCTAssertEqual(stats.shardDetails.count, 1, "Deve haver 1 detalhe (1 shard)")
    }
    
    func testGetGlobalStats() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine sem particionamento
        let storage = try StorageEngine(path: tempDirectory.path, shardKey: nil, compressionMethod: .none)
        
        // Insere documentos em duas coleções
        let collection1 = "Users"
        let collection2 = "Orders"
        
        let users = [
            TestModel(id: 1, name: "Alice", category: nil),
            TestModel(id: 2, name: "Bob", category: nil)
        ]
        let orders = [
            TestModel(id: 101, name: "Order1", category: nil),
            TestModel(id: 102, name: "Order2", category: nil),
            TestModel(id: 103, name: "Order3", category: nil)
        ]
        
        for user in users {
            try await storage.insertDocument(user, collection: collection1)
        }
        for order in orders {
            try await storage.insertDocument(order, collection: collection2)
        }
        
        let statsEngine = StatsEngine(storage: storage)
        let globalStats = try await statsEngine.getGlobalStats()
        
        XCTAssertEqual(globalStats.totalCollections, 2, "Deve haver 2 coleções")
        XCTAssertEqual(globalStats.totalDocuments, users.count + orders.count, "O total de documentos deve ser a soma")
        XCTAssertTrue(globalStats.totalSizeInBytes > 0, "O tamanho global deve ser maior que zero")
    }
}
