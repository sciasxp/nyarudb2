import XCTest

@testable import NyaruDB2

final class StorageEngineTests: XCTestCase {

    struct TestModel: Codable, Equatable {
        let id: Int
        let name: String
        // Se você for usar particionamento, inclua o campo da shardKey, por exemplo:
        let category: String?
    }

    func testInsertAndFetchWithoutPartition() async throws {
        // Sem particionamento: shardKey nil
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("StorageEngineTest")
        try? FileManager.default.removeItem(at: tempURL)
        let storage = StorageEngine(
            path: tempURL.path,
            shardKey: nil,
            compressionMethod: .none
        )

        let model = TestModel(id: 1, name: "Exemplo", category: nil)
        try await storage.insertDocument(model, collection: "TestCollection")

        // Caso tenha um método de fetch implementado, você pode testar aqui
        // Por exemplo: let fetched = try await storage.fetchDocuments(from: "TestCollection")
        // XCTAssertEqual(fetched.count, 1)
    }

    func testInsertAndFetchWithPartition() async throws {
        // Com particionamento: usando o campo "category" como chave
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("StorageEngineTestPartition")
        try? FileManager.default.removeItem(at: tempURL)
        let storage = StorageEngine(
            path: tempURL.path,
            shardKey: "category",
            compressionMethod: .lzfse
        )

        let model1 = TestModel(id: 1, name: "Item 1", category: "A")
        let model2 = TestModel(id: 2, name: "Item 2", category: "B")
        let model3 = TestModel(id: 3, name: "Item 3", category: "A")

        try await storage.insertDocument(model1, collection: "PartitionTest")
        try await storage.insertDocument(model2, collection: "PartitionTest")
        try await storage.insertDocument(model3, collection: "PartitionTest")

        // Aqui você pode implementar ou simular um método de busca por shard, por exemplo:
        // let itemsA: [TestModel] = try await storage.fetchDocuments(whereShardValue: "A", collection: "PartitionTest")
        // XCTAssertEqual(itemsA.count, 2)
    }
}
