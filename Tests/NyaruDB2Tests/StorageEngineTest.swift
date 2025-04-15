import XCTest

@testable import NyaruDB2

// Modelo para testes
// struct StorageEngineTestModel: Codable, Equatable {
//     let id: Int
//     let name: String
//     let category: String?  // Usado para particionamento, se necessário
// }

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

        // Inicializa o StorageEngine sem particionamento (usa "default" para todos os documentos)
        // Aqui, para o teste, usamos FileProtection.none (definido via enum ou typealias, conforme sua implementação)
        storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none,
            fileProtectionType: FileProtectionType.none
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

        // Inicializa o StorageEngine sem particionamento.
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )
        let model = TestModel(id: 1, name: "Test", category: nil)

        try await storage.insertDocument(model, collection: "TestCollection")

        // O StorageEngine utiliza o shard "default" quando não há shardKey.
        let fileURL =
            tempDirectory
            .appendingPathComponent("TestCollection", isDirectory: true)
            .appendingPathComponent("default.nyaru")
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileURL.path),
            "O arquivo do shard 'default.nyaru' deve existir."
        )

        let data = try Data(contentsOf: fileURL)
        let decoded = try JSONDecoder().decode([TestModel].self, from: data)
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

        // Inicializa StorageEngine com particionamento usando "category" e indexando pelo campo "name".
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: "category",
            compressionMethod: .none,
            fileProtectionType: .none
        )

        let modelA = TestModel(id: 1, name: "Alice", category: "A")
        let modelB = TestModel(id: 2, name: "Bob", category: "B")
        let modelA2 = TestModel(id: 3, name: "Alice", category: "A")

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

    // Teste para quando o documento NÃO possui a chave de particionamento
    func testInsertDocumentMissingPartitionKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine com particionamento usando "category".
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: "category",
            compressionMethod: .none,
            fileProtectionType: .none
        )

        // Cria um modelo sem a propriedade "category"
        struct ModelMissingPartition: Codable, Equatable {
            let id: Int
            let name: String
        }
        let model = ModelMissingPartition(id: 1, name: "NoCategory")

        // Como o documento não contém "category", deve lançar um erro
        do {
            try await storage.insertDocument(
                model,
                collection: "TestCollection"
            )
            XCTFail("Expected error not thrown")
        } catch {
            guard
                case StorageEngine.StorageError.partitionKeyNotFound(let key) =
                    error
            else {
                XCTFail("Erro inesperado: \(error)")
                return
            }
            XCTAssertEqual(key, "category")
        }
    }

    // Teste para quando o documento NÃO possui o campo de índice solicitado
    func testInsertDocumentMissingIndexKey() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa StorageEngine com particionamento usando "category".
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: "category",
            compressionMethod: .none,
            fileProtectionType: .none
        )

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
            guard
                case StorageEngine.StorageError.indexKeyNotFound(let key) =
                    error
            else {
                XCTFail("Erro inesperado: \(error)")
                return
            }
            XCTAssertEqual(key, "name")
        }
    }

    // Teste de inserção múltipla e validação do conteúdo do shard
    func testMultipleInsertionsAndContent() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem particionamento
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        let model1 = TestModel(id: 1, name: "Test1", category: nil)
        let model2 = TestModel(id: 2, name: "Test2", category: nil)
        let model3 = TestModel(id: 3, name: "Test3", category: nil)

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
        let decoded = try JSONDecoder().decode([TestModel].self, from: data)
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

        // Inicializa o StorageEngine sem particionamento (usa "default")
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        // Cria alguns modelos de teste
        let model1 = TestModel(id: 1, name: "One", category: nil)
        let model2 = TestModel(id: 2, name: "Two", category: nil)
        let model3 = TestModel(id: 3, name: "Three", category: nil)

        // Insere os documentos
        try await storage.insertDocument(model1, collection: "MyCollection")
        try await storage.insertDocument(model2, collection: "MyCollection")
        try await storage.insertDocument(model3, collection: "MyCollection")

        // Agora utiliza o método fetchDocuments otimizado para buscar todos os documentos da coleção
        let fetched: [TestModel] = try await storage.fetchDocuments(
            from: "MyCollection"
        )

        // Valida se o número de documentos é o esperado
        XCTAssertEqual(fetched.count, 3)
        // Se a ordem for garantida pela estratégia de append, podemos verificar também:
        XCTAssertEqual(fetched, [model1, model2, model3])
    }

    func testDeleteDocument() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa StorageEngine sem particionamento
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        let model1 = TestModel(id: 1, name: "Alice", category: nil)
        let model2 = TestModel(id: 2, name: "Bob", category: nil)
        let model3 = TestModel(id: 3, name: "Alice", category: nil)

        // Inserir três documentos
        try await storage.insertDocument(model1, collection: "Users")
        try await storage.insertDocument(model2, collection: "Users")
        try await storage.insertDocument(model3, collection: "Users")

        // Agora, deleta os documentos cujo nome seja "Alice"
        try await storage.deleteDocuments(
            where: { (doc: TestModel) -> Bool in
                return doc.name == "Alice"
            },
            from: "Users"
        )

        // Verifica que apenas o documento de Bob permanece
        let remaining: [TestModel] = try await storage.fetchDocuments(
            from: "Users"
        )
        XCTAssertEqual(remaining.count, 1)
        XCTAssertEqual(remaining.first?.name, "Bob")
    }

    func testUpdateDocumentWithoutPartitionAndIndex() async throws {
        // Cria um diretório temporário para isolar os dados do teste.
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem shardKey (todos os documentos vão para o shard "default")
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        // Insere dois documentos
        let model1 = TestModel(id: 1, name: "Alice", category: nil)
        let model2 = TestModel(id: 2, name: "Bob", category: nil)

        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")

        // Cria um documento atualizado para model1 (por exemplo, altera o nome para "Alicia")
        let updatedModel1 = TestModel(
            id: 1,
            name: "Alicia",
            category: nil
        )

        // Chama a função updateDocument
        try await storage.updateDocument(
            updatedModel1,
            in: "TestCollection",
            matching: { (doc: TestModel) -> Bool in
                return doc.id == 1
            }
        )

        // Recupera os documentos da coleção usando fetchDocuments
        let fetched: [TestModel] = try await storage.fetchDocuments(
            from: "TestCollection"
        )

        // Verifica se os documentos foram atualizados corretamente
        XCTAssertEqual(
            fetched.count,
            2,
            "Devem existir 2 documentos após o update"
        )

        // Verifica que o documento com id 1 foi atualizado
        guard let model = fetched.first(where: { $0.id == 1 }) else {
            XCTFail("Documento com id 1 não foi encontrado")
            return
        }
        XCTAssertEqual(
            model.name,
            "Alicia",
            "O nome do documento atualizado deve ser 'Alicia'"
        )
    }

    func testBulkInsertDocuments() async throws {
        // Cria um diretório temporário para isolar os dados do teste.
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa StorageEngine com particionamento usando "category" e sem compressão para facilitar os testes.
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: "category",
            compressionMethod: .none,
            fileProtectionType: .none
        )

        // Define um array de documentos para inserir
        let documents = [
            TestModel(id: 1, name: "Alice", category: "A"),
            TestModel(id: 2, name: "Bob", category: "B"),
            TestModel(id: 3, name: "Charlie", category: "A"),
            TestModel(id: 4, name: "David", category: "C"),
        ]

        // Chama o método bulkInsertDocuments na coleção "BulkTestCollection"
        try await storage.bulkInsertDocuments(
            documents,
            collection: "BulkTestCollection",
            indexField: "name"
        )

        // Verifica se os documentos foram distribuídos corretamente nos shards.
        // Para isso, espera que os arquivos "A.nyaru", "B.nyaru" e "C.nyaru" existam na coleção.
        let collectionURL = tempDirectory.appendingPathComponent(
            "BulkTestCollection",
            isDirectory: true
        )
        let fileA = collectionURL.appendingPathComponent("A.nyaru")
        let fileB = collectionURL.appendingPathComponent("B.nyaru")
        let fileC = collectionURL.appendingPathComponent("C.nyaru")

        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileA.path),
            "O arquivo do shard 'A.nyaru' deve existir."
        )
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileB.path),
            "O arquivo do shard 'B.nyaru' deve existir."
        )
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: fileC.path),
            "O arquivo do shard 'C.nyaru' deve existir."
        )

        // Recupera todos os documentos usando o método fetchDocuments (que realiza full scan dos shards)
        let fetched: [TestModel] = try await storage.fetchDocuments(
            from: "BulkTestCollection"
        )

        // Valida se o número de documentos é o esperado e se eles correspondem aos inseridos.
        XCTAssertEqual(fetched.count, documents.count)
        // Ordene os resultados por id para comparar, se a ordem não for garantida.
        XCTAssertEqual(
            fetched.sorted(by: { $0.id < $1.id }),
            documents.sorted(by: { $0.id < $1.id }),
            "Os documentos recuperados devem corresponder aos inseridos."
        )
    }

    func testCountDocuments() async throws {
        // Cria um diretório temporário
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem shardKey (todos os documentos vão para o shard "default")
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        // Insere três documentos na coleção "CountCollection"
        let model1 = TestModel(id: 1, name: "Alice", category: nil)
        let model2 = TestModel(id: 2, name: "Bob", category: nil)
        let model3 = TestModel(id: 3, name: "Charlie", category: nil)

        try await storage.insertDocument(model1, collection: "CountCollection")
        try await storage.insertDocument(model2, collection: "CountCollection")
        try await storage.insertDocument(model3, collection: "CountCollection")

        // Chama o método countDocuments e espera o total ser 3
        let count = try await storage.countDocuments(in: "CountCollection")
        XCTAssertEqual(
            count,
            3,
            "A contagem de documentos na coleção deve ser 3"
        )
    }

    func testDropCollection() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine (sem shardKey para simplificar)
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        // Insere documentos em "DropCollection"
        let model1 = TestModel(id: 1, name: "Alice", category: nil)
        let model2 = TestModel(id: 2, name: "Bob", category: nil)

        try await storage.insertDocument(model1, collection: "DropCollection")
        try await storage.insertDocument(model2, collection: "DropCollection")

        // Verifica que o diretório da coleção existe
        let collectionURL = tempDirectory.appendingPathComponent(
            "DropCollection",
            isDirectory: true
        )
        XCTAssertTrue(
            FileManager.default.fileExists(atPath: collectionURL.path),
            "O diretório da coleção 'DropCollection' deve existir"
        )

        // Executa o drop da coleção
        try await storage.dropCollection("DropCollection")

        // Verifica que o diretório foi removido
        XCTAssertFalse(
            FileManager.default.fileExists(atPath: collectionURL.path),
            "O diretório da coleção 'DropCollection' não deve existir após o drop"
        )

        // Para segurança, a contagem de documentos também deve ser 0
        let count = try await storage.countDocuments(in: "DropCollection")
        XCTAssertEqual(
            count,
            0,
            "Depois de dropar a coleção, a contagem de documentos deve ser 0"
        )
    }

    func testListCollections() async throws {
        // Cria um diretório temporário para o teste.
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )

        // Cria alguns subdiretórios que simulam coleções.
        let collections = ["Users", "Orders", "Products"]
        for collection in collections {
            let collectionURL = tempDirectory.appendingPathComponent(
                collection,
                isDirectory: true
            )
            try FileManager.default.createDirectory(
                at: collectionURL,
                withIntermediateDirectories: true
            )
        }

        // Inicializa um StorageEngine com o diretório base criado
        let storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,
            compressionMethod: .none
        )

        // Chama a função listCollections
        let listedCollections = try await storage.listCollections()

        // Como o FileManager pode não garantir a ordem, ordenamos antes de comparar
        XCTAssertEqual(listedCollections.sorted(), collections.sorted())
    }

    func testFetchDocumentsStream() async throws {
        // Inserir documentos de teste na coleção "TestCollection"
        let model1 = TestModel(id: 1, name: "One", category: nil)
        let model2 = TestModel(id: 2, name: "Two", category: nil)
        let model3 = TestModel(id: 3, name: "Three", category: nil)

        try await storage.insertDocument(model1, collection: "TestCollection")
        try await storage.insertDocument(model2, collection: "TestCollection")
        try await storage.insertDocument(model3, collection: "TestCollection")

        // Usa o método fetchDocumentsStream para recuperar os documentos
        let stream: AsyncThrowingStream<TestModel, Error> =
            await storage.fetchDocumentsLazy(from: "TestCollection")

        // Coleta os documentos emitidos pelo stream incrementalmente
        var results: [TestModel] = []
        for try await document in stream {
            results.append(document)
        }

        // Valida se a contagem e o conteúdo são os esperados.
        // Como a ordem pode não ser garantida, vamos validar pelo conteúdo.
        XCTAssertEqual(results.count, 3, "Deve retornar 3 documentos")

        XCTAssertTrue(
            results.contains(model1),
            "Deve conter o documento model1"
        )
        XCTAssertTrue(
            results.contains(model2),
            "Deve conter o documento model2"
        )
        XCTAssertTrue(
            results.contains(model3),
            "Deve conter o documento model3"
        )
    }

}
