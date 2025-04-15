import XCTest
@testable import NyaruDB2

final class CollectionManagerTests: XCTestCase {
    
    var db: NyaruDB2!
    var tempPath: String!
    
    override func setUp() async throws {
        // Cria um caminho temporário único para o teste
        tempPath = NSTemporaryDirectory().appending(UUID().uuidString)
        // Inicializa o NyaruDB2 sem uma chave de partição global, pois usaremos a configuração por coleção
        db = try NyaruDB2(path: tempPath, shardKey: nil, compressionMethod: .none, fileProtectionType: .none)
    }
    
    override func tearDown() async throws {
        // Limpa os arquivos do ambiente de teste
        try? FileManager.default.removeItem(atPath: tempPath)
    }
    
    /// Testa a criação de uma coleção e a sua recuperação pelo nome.
    func testCreateAndRetrieveCollection() async throws {
        let manager = CollectionManager.shared
        let collection = manager.createCollection(db: db, name: "TestCollection", indexes: ["id"], partitionKey: "created_at")
        XCTAssertNotNil(collection, "A coleção criada não deve ser nula")
        
        let retrieved = manager.getCollection(named: "TestCollection")
        XCTAssertNotNil(retrieved, "A coleção recuperada não deve ser nula")
        XCTAssertEqual(collection.metadata.name, retrieved?.metadata.name, "Os nomes das coleções devem ser iguais")
        XCTAssertEqual(collection.metadata.partitionKey, retrieved?.metadata.partitionKey, "As chaves de partição devem ser iguais")
    }
    
    /// Testa a listagem de coleções registradas no CollectionManager.
    func testListCollections() async throws {
        let manager = CollectionManager.shared
        
        // Cria duas coleções de teste
        _ = manager.createCollection(db: db, name: "Collection1", indexes: ["id"], partitionKey: "created_at")
        _ = manager.createCollection(db: db, name: "Collection2", indexes: ["id"], partitionKey: "created_at")
        
        let collections = manager.listCollections()
        XCTAssertGreaterThanOrEqual(collections.count, 2, "Deve haver pelo menos 2 coleções registradas")
        
        let names = collections.map { $0.metadata.name }
        XCTAssertTrue(names.contains("Collection1"), "A lista de coleções deve conter 'Collection1'")
        XCTAssertTrue(names.contains("Collection2"), "A lista de coleções deve conter 'Collection2'")
    }
}
