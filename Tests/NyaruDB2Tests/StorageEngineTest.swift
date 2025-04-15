import XCTest
@testable import NyaruDB2

struct StorageModel: Codable, Equatable {
    let id: Int
    let name: String
    let category: String?  // Usado para particionamento
}

final class StorageEngineTests: XCTestCase {

    var tempDirectory: URL!
    var storage: StorageEngine!

    override func setUp() async throws {
        // Cria um diretório temporário para isolar o teste
        tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem configuração global de particionamento.
        // Agora o particionamento por coleção será definido via o dicionário collectionPartitionKeys.
        storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )
    }

    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: tempDirectory)
    }

    func testInsertDocumentWithoutPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem particionamento (dicionário vazio)
        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )
        let model = StorageModel(id: 1, name: "Test", category: nil)

        try await storage.insertDocument(model, collection: "TestCollection")

        // Como não foi definida chave de partição, usa "default" como shard.
        let fileURL =
            tempDirectory
            .appendingPathComponent("TestCollection", isDirectory: true)
            .appendingPathComponent("default.nyaru")
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileURL.path),
            "O arquivo do shard 'default.nyaru' deve existir."
        )

        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([StorageModel].self, from: data)
        XCTAssertEqual(decoded.count, 1)
        XCTAssertEqual(decoded.first, model)
    }

    func testInsertDocumentWithPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )
        
        // Inicializa o StorageEngine sem particionamento global.
        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )

        await storage.setPartitionKey(for: "TestCollection", key: "category")

        let modelA = StorageModel(id: 1, name: "Alice", category: "A")
        let modelB = StorageModel(id: 2, name: "Bob", category: "B")
        let modelA2 = StorageModel(id: 3, name: "Alice", category: "A")

        try await storage.insertDocument(
            modelA,
            collection: "TestCollection",
            indexField: "name"
        )
        try await storage.insertDocument(
            modelB,
            collection: "TestCollection",
            indexField: "name"
        )
        try await storage.insertDocument(
            modelA2,
            collection: "TestCollection",
            indexField: "name"
        )

        let collectionURL = tempDirectory.appendingPathComponent(
            "TestCollection",
            isDirectory: true
        )
        let fileA = collectionURL.appendingPathComponent("A.nyaru")
        let fileB = collectionURL.appendingPathComponent("B.nyaru")
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileA.path),
            "O arquivo do shard 'A.nyaru' deve existir."
        )
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileB.path),
            "O arquivo do shard 'B.nyaru' deve existir."
        )
    }

    // Teste para quando o documento NÃO possui a chave de particionamento configurada na coleção
    func testInsertDocumentMissingPartitionKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine com dicionário de particionamento vazio para "TestCollection"
        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )
        // Aqui, não definimos uma chave de particionamento para "TestCollection".
        // Cria um modelo sem a propriedade "category"
        struct ModelMissingPartition: Codable, Equatable {
            let id: Int
            let name: String
        }
        let model = ModelMissingPartition(id: 1, name: "NoCategory")

        // Como não há configuração de partição, o StorageEngine usará "default" e não lançará erro.
        try await storage.insertDocument(
            model,
            collection: "TestCollection"
        )
        // Se necessário, o teste pode ser ajustado para lançar erro caso desejemos forçar a presença da chave via outra lógica.
    }

    // Teste para quando o documento NÃO possui o campo de índice solicitado
    func testInsertDocumentMissingIndexKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )
        
        // Inicializa o StorageEngine e define particionamento para "TestCollection"
        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )
        
        await storage.setPartitionKey(for: "TestCollection", key: "category")

        // Modelo com chave de particionamento, mas sem a chave "name" (será usada para index)
        struct ModelMissingIndex: Codable, Equatable {
            let id: Int
            let category: String
        }
        let model = ModelMissingIndex(id: 1, category: "X")

        do {
            try await storage.insertDocument(
                model,
                collection: "TestCollection",
                indexField: "name"
            )
            XCTFail("Expected error not thrown")
        } catch {
            // Supondo que o DynamicDecoder lance StorageError.indexKeyNotFound quando a chave não é encontrada.
            guard
                case StorageEngine.StorageError.indexKeyNotFound(let key) = error
            else {
                XCTFail("Unexpected error: \(error)")
                return
            }
            XCTAssertEqual(key, "name")
        }
    }

    // Teste de inserção múltipla e validação do conteúdo do shard sem particionamento (usa "default")
    func testMultipleInsertionsAndContent() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "Test1", category: nil)
        let model2 = StorageModel(id: 2, name: "Test2", category: nil)
        let model3 = StorageModel(id: 3, name: "Test3", category: nil)

        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")
        try await storage.insertDocument(model3, collection: "TestCollection")

        let fileURL =
            tempDirectory
            .appendingPathComponent("TestCollection", isDirectory: true)
            .appendingPathComponent("default.nyaru")
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileURL.path),
            "O arquivo do shard 'default.nyaru' deve existir."
        )

        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([StorageModel].self, from: data)
        XCTAssertEqual(decoded.count, 3)
        XCTAssertEqual(decoded, [model1, model2, model3])
    }

    func testFetchDocuments() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "One", category: nil)
        let model2 = StorageModel(id: 2, name: "Two", category: nil)
        let model3 = StorageModel(id: 3, name: "Three", category: nil)

        try await storage.insertDocument(model1, collection: "MyCollection")
        try await storage.insertDocument(model2, collection: "MyCollection")
        try await storage.insertDocument(model3, collection: "MyCollection")

        let fetched: [StorageModel] = try await storage.fetchDocuments(from: "MyCollection")
        XCTAssertEqual(fetched.count, 3)
        XCTAssertEqual(fetched, [model1, model2, model3])
    }

    func testDeleteDocument() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "Alice", category: nil)
        let model2 = StorageModel(id: 2, name: "Bob", category: nil)
        let model3 = StorageModel(id: 3, name: "Alice", category: nil)

        try await storage.insertDocument(model1, collection: "Users")
        try await storage.insertDocument(model2, collection: "Users")
        try await storage.insertDocument(model3, collection: "Users")

        try await storage.deleteDocuments(where: { (doc: StorageModel) -> Bool in
            return doc.name == "Alice"
        }, from: "Users")

        let remaining: [StorageModel] = try await storage.fetchDocuments(from: "Users")
        XCTAssertEqual(remaining.count, 1)
        XCTAssertEqual(remaining.first?.name, "Bob")
    }

    func testUpdateDocumentWithoutPartitionAndIndex() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "Alice", category: nil)
        let model2 = StorageModel(id: 2, name: "Bob", category: nil)

        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")

        let updatedModel1 = StorageModel(id: 1, name: "Alicia", category: nil)
        try await storage.updateDocument(
            updatedModel1,
            in: "TestCollection",
            matching: { (doc: StorageModel) -> Bool in
                return doc.id == 1
            }
        )

        let fetched: [StorageModel] = try await storage.fetchDocuments(from: "TestCollection")
        XCTAssertEqual(fetched.count, 2)

        guard let model = fetched.first(where: { $0.id == 1 }) else {
            XCTFail("Documento com id 1 não foi encontrado")
            return
        }
        XCTAssertEqual(model.name, "Alicia")
    }

    func testBulkInsertDocuments() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa StorageEngine e define particionamento via dicionário para "BulkTestCollection"
        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )
        await storage.setPartitionKey(for: "BulkTestCollection", key: "category")

        let documents = [
            StorageModel(id: 1, name: "Alice", category: "A"),
            StorageModel(id: 2, name: "Bob", category: "B"),
            StorageModel(id: 3, name: "Charlie", category: "A"),
            StorageModel(id: 4, name: "David", category: "C"),
        ]

        try await storage.bulkInsertDocuments(documents, collection: "BulkTestCollection", indexField: "name")

        let collectionURL = tempDirectory.appendingPathComponent("BulkTestCollection", isDirectory: true)
        let fileA = collectionURL.appendingPathComponent("A.nyaru")
        let fileB = collectionURL.appendingPathComponent("B.nyaru")
        let fileC = collectionURL.appendingPathComponent("C.nyaru")

        XCTAssertTrue(FileManager.default.fileExists(atPath: fileA.path), "O arquivo do shard 'A.nyaru' deve existir.")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileB.path), "O arquivo do shard 'B.nyaru' deve existir.")
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileC.path), "O arquivo do shard 'C.nyaru' deve existir.")

        let fetched: [StorageModel] = try await storage.fetchDocuments(from: "BulkTestCollection")
        XCTAssertEqual(fetched.count, documents.count)
        XCTAssertEqual(
            fetched.sorted(by: { $0.id < $1.id }),
            documents.sorted(by: { $0.id < $1.id }),
            "Os documentos recuperados devem corresponder aos inseridos."
        )
    }

    func testCountDocuments() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "Alice", category: nil)
        let model2 = StorageModel(id: 2, name: "Bob", category: nil)
        let model3 = StorageModel(id: 3, name: "Charlie", category: nil)

        try await storage.insertDocument(model1, collection: "CountCollection")
        try await storage.insertDocument(model2, collection: "CountCollection")
        try await storage.insertDocument(model3, collection: "CountCollection")

        let count = try await storage.countDocuments(in: "CountCollection")
        XCTAssertEqual(count, 3, "A contagem de documentos na coleção deve ser 3")
    }

    func testDropCollection() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let model1 = StorageModel(id: 1, name: "Alice", category: nil)
        let model2 = StorageModel(id: 2, name: "Bob", category: nil)

        try await storage.insertDocument(model1, collection: "DropCollection")
        try await storage.insertDocument(model2, collection: "DropCollection")

        let collectionURL = tempDirectory.appendingPathComponent("DropCollection", isDirectory: true)
        XCTAssertTrue(FileManager.default.fileExists(atPath: collectionURL.path), "O diretório da coleção 'DropCollection' deve existir")

        try await storage.dropCollection("DropCollection")

        XCTAssertFalse(FileManager.default.fileExists(atPath: collectionURL.path), "O diretório da coleção 'DropCollection' não deve existir após o drop")

        let count = try await storage.countDocuments(in: "DropCollection")
        XCTAssertEqual(count, 0, "Depois de dropar a coleção, a contagem de documentos deve ser 0")
    }

    func testListCollections() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        let collections = ["Users", "Orders", "Products"]
        for collection in collections {
            let collectionURL = tempDirectory.appendingPathComponent(collection, isDirectory: true)
            try FileManager.default.createDirectory(at: collectionURL, withIntermediateDirectories: true)
        }

        let storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none
        )

        let listedCollections = try await storage.listCollections()
        XCTAssertEqual(listedCollections.sorted(), collections.sorted())
    }

    func testFetchDocumentsStream() async throws {
        let model1 = StorageModel(id: 1, name: "One", category: nil)
        let model2 = StorageModel(id: 2, name: "Two", category: nil)
        let model3 = StorageModel(id: 3, name: "Three", category: nil)

        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")
        try await storage.insertDocument(model3, collection: "TestCollection")

        let stream: AsyncThrowingStream<StorageModel, Error> = await storage.fetchDocumentsLazy(from: "TestCollection")
        var results: [StorageModel] = []
        for try await document in stream {
            results.append(document)
        }

        XCTAssertEqual(results.count, 3, "Deve retornar 3 documentos")
        XCTAssertTrue(results.contains(model1), "Deve conter model1")
        XCTAssertTrue(results.contains(model2), "Deve conter model2")
        XCTAssertTrue(results.contains(model3), "Deve conter model3")
    }
}
