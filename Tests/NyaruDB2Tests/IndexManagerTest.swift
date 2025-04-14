import XCTest

@testable import NyaruDB2

final class IndexManagerTests: XCTestCase {

    func testCreateIndexAndInsertSearch() async {
        let manager = IndexManager<String>()
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
        let manager = IndexManager<String>()
        // Se o índice para o campo "age" não foi criado, a busca deve retornar vazio
        let results = await manager.search("age", value: "30")
        XCTAssertEqual(results.count, 0)
    }
}
