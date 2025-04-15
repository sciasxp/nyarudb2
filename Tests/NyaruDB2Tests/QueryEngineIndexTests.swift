//
//  QueryEngineIndexTest.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 14/04/25.
//

import XCTest

@testable import NyaruDB2


final class QueryEngineIndexTests: XCTestCase {

    // Testa o fetchFromIndex diretamente no StorageEngine
    func testFetchFromIndex() async throws {
        // Cria um diretório temporário isolado para o teste
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: tempDir,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem particionamento
        let storage = try StorageEngine(
            path: tempDir.path,
            shardKey: nil,
            compressionMethod: .none,
            fileProtectionType: .none
        )

        let collection = "Users"
        let docs: [Users] = [
            Users(id: 1, name: "Alice", age: 30),
            Users(id: 2, name: "Bob", age: 25),
            Users(id: 3, name: "Alice", age: 40),
        ]

        // Insere os documentos com índice baseado no campo "name"
        for doc in docs {
            try await storage.insertDocument(
                doc,
                collection: collection,
                indexField: "name"
            )
        }

        // Chama o fetchFromIndex para buscar documentos onde 'name' seja "Alice"
        let result: [TestModel] = try await storage.fetchFromIndex(
            collection: collection,
            field: "name",
            value: "Alice"
        )

        // Verifica se foram retornados os 2 documentos esperados
        XCTAssertEqual(result.count, 2)
        XCTAssertTrue(result.contains(where: { $0.id == 1 }))
        XCTAssertTrue(result.contains(where: { $0.id == 3 }))

        // Limpa o diretório temporário
        try? FileManager.default.removeItem(at: tempDir)
    }

    // Testa a estratégia executeIndexOnly do Query<T>
    func testExecuteIndexOnly() async throws {
        // Cria um diretório temporário isolado para o teste
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(
            at: tempDir,
            withIntermediateDirectories: true
        )

        // Inicializa o StorageEngine sem particionamento
        let storage = try StorageEngine(
            path: tempDir.path,
            shardKey: nil,
            compressionMethod: .none,
            fileProtectionType: .none
        )

        let collection = "TestCollection"
        let docs: [Users] = [
            Users(id: 1, name: "Alice", age: 30),
            Users(id: 2, name: "Bob", age: 25),
            Users(id: 3, name: "Alice", age: 40),
        ]

        // Insere os documentos com índice (campo "name")
        for doc in docs {
            try await storage.insertDocument(
                doc,
                collection: collection,
                indexField: "name"
            )
        }

        // Cria estatísticas dummy para forçar a estratégia indexOnly
        // Nesse exemplo, para o índice "name", consideramos que o custo de buscar "Alice" seja baixo.
        let dummyStat = IndexStat(
            totalCount: 2,
            uniqueValuesCount: 1,
            valueDistribution: ["Alice": 2],
            accessCount: 0,
            lastAccess: Date()
        )
        let indexStats: [String: IndexStat] = ["name": dummyStat]
        let shardStats: [ShardStat] = []  // dummy; não afeta esse teste

        // Cria a query para a coleção "TestCollection"
        var query = Query<TestModel>(
            collection: collection,
            storage: storage,
            indexStats: indexStats,
            shardStats: shardStats
        )
        // Adiciona o predicado de igualdade para o campo "name"
        query.where(\TestModel.name, .equal("Alice"))

        // Executa a query usando a estratégia indexOnly (via fetchFromIndex)
        let results = try await query.execute()

        // Verifica se o resultado contém os 2 documentos com nome "Alice"
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results.allSatisfy { $0.name == "Alice" })

        try? FileManager.default.removeItem(at: tempDir)
    }
}
