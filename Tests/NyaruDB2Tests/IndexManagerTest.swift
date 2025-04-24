//
//  IndexManagerTest.swift
//  NyaruDB2
//
//  Created by demetrius albuquerque on 11/04/25.
//

import XCTest
@testable import NyaruDB2

final class IndexManagerTests: XCTestCase {

    var manager: IndexManager<String>!
    
    override func setUp() async throws {
        manager = IndexManager<String>()
    }
    
    override func tearDown() async throws {
        manager = nil
    }

    func testCreateIndexAndInsertSearch() async {
        // Cria um índice para o campo "name"
        await manager.createIndex(for: "name", minimumDegree: 2)

        let data1 = "Record1".data(using: .utf8)!
        let data2 = "Record2".data(using: .utf8)!

        // Insere dois registros com a chave "Alice"
        await manager.insert(index: "name", key: "Alice", data: data1)
        await manager.insert(index: "name", key: "Alice", data: data2)

        // Realiza a busca e verifica se os dados foram inseridos corretamente
        let results = await manager.search("name", value: "Alice")
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results.contains(data1))
        XCTAssertTrue(results.contains(data2))
    }

    func testSearchWithoutIndex() async {
        // Se o índice para o campo "age" não foi criado, a busca deve retornar vazio
        let results = await manager.search("age", value: "30")
        XCTAssertEqual(results.count, 0)
    }
    
    func testListIndexesAndDropIndex() async throws {
        // 1. Nenhum índice no início
        let initialIndex = await manager.listIndexes()
        XCTAssertTrue(initialIndex.isEmpty, "Inicialmente não deve haver índices.")

        // 2. Cria dois índices: "name" e "category"
        await manager.createIndex(for: "name")
        await manager.createIndex(for: "category")
        
        // 3. Verifica se listIndexes retorna os campos criados
        let indexes = await manager.listIndexes()
        XCTAssertEqual(Set(indexes), Set(["name", "category"]), "Deve conter 'name' e 'category' como índices.")

        // 4. Remove o índice "name"
        let removed = await manager.dropIndex(for: "name")
        XCTAssertTrue(removed, "Remover o índice 'name' deve retornar true, pois o índice existia.")

        // 5. Verifica que agora só sobra "category"
        let updatedIndexes = await manager.listIndexes()
        XCTAssertEqual(updatedIndexes, ["category"], "Após remover 'name', deve restar apenas 'category'.")

        // 6. Remove índice inexistente
        let removedNonExistent = await manager.dropIndex(for: "id")
        XCTAssertFalse(removedNonExistent, "Remover um índice inexistente deve retornar false.")

        // 7. Verifica que ainda existe 'category'
        let finalIndexes = await manager.listIndexes()
        XCTAssertEqual(finalIndexes, ["category"], "Após tentar remover índice inexistente, 'category' continua presente.")
    }
}
