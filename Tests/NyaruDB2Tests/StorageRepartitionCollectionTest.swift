//
//  StorageRepartitionCollectionTest.swift
//  NyaruDB2
//
//  Created by demetrius albuquerque on 13/04/25.
//

import XCTest
@testable import NyaruDB2

struct RepartitionDocument: Codable, Equatable {
    let id: Int
    let name: String
    let newField: String  // Usado como nova chave de partição
}

final class StorageRepartitionCollectionTests: XCTestCase {
    var tempDirectory: URL!
    var storage: StorageEngine!
    
    override func setUp() async throws {
        // Cria um diretório temporário único para o teste
        tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine (sem shardKey global, pois usaremos a função repartitionCollection)
        storage = try StorageEngine(
            path: tempDirectory.path,
            compressionMethod: .none,
            fileProtectionType: .none
        )
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: tempDirectory)
    }
    
    func testRepartitionCollection() async throws {
        // 1. Inserir documentos na coleção "TestCollection" sem particionamento configurado.
        // Todos os documentos serão inicialmente inseridos no shard "default.nyaru"
        let docs = [
            RepartitionDocument(id: 1, name: "Doc1", newField: "A"),
            RepartitionDocument(id: 2, name: "Doc2", newField: "B"),
            RepartitionDocument(id: 3, name: "Doc3", newField: "A"),
            RepartitionDocument(id: 4, name: "Doc4", newField: "B")
        ]
        try await storage.bulkInsertDocuments(docs, collection: "TestCollection", indexField: nil)
        
        // Verifica que antes da repartição o shard "default.nyaru" existe
        let collectionDir = tempDirectory.appendingPathComponent("TestCollection", isDirectory: true)
        let defaultShardURL = collectionDir.appendingPathComponent("default.nyaru")
        XCTAssertTrue(FileManager.default.fileExists(atPath: defaultShardURL.path),
                      "Antes da repartição, o shard 'default.nyaru' deve existir.")
        
        // 2. Chama repartitionCollection com a nova chave de partição "newField"
        try await storage.repartitionCollection(collection: "TestCollection", newPartitionKey: "newField", as: RepartitionDocument.self)
        
        // 3. Verifica que os shards "A.nyaru" e "B.nyaru" foram criados, ou seja, os documentos foram distribuídos
        let shardAURL = collectionDir.appendingPathComponent("A.nyaru")
        let shardBURL = collectionDir.appendingPathComponent("B.nyaru")
        XCTAssertTrue(FileManager.default.fileExists(atPath: shardAURL.path),
                      "Após repartition, o arquivo do shard 'A.nyaru' deve existir.")
        XCTAssertTrue(FileManager.default.fileExists(atPath: shardBURL.path),
                      "Após repartition, o arquivo do shard 'B.nyaru' deve existir.")
        
        // 4. Verifica que o arquivo de metadados agregado foi gerado e contém as informações corretas.
        let aggregatedMetaURL = collectionDir.appendingPathComponent("TestCollection.nyaru.meta.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: aggregatedMetaURL.path),
                      "O arquivo de metadados agregado deve existir.")
        
        let metaData = try Data(contentsOf: aggregatedMetaURL)
        let stats = try JSONDecoder().decode(CollectionStats.self, from: metaData)
        XCTAssertEqual(stats.totalDocuments, docs.count,
                       "O total de documentos deve ser igual ao número de documentos inseridos.")
        XCTAssertEqual(stats.numberOfShards, 2,
                       "O número de shards ativos deve ser 2 (A e B).")
    }
}
