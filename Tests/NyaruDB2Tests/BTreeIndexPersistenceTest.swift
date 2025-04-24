//
//  BTreeIndexPersistenceTest.swift
//  NyaruDB2
//
//  Created by demetrius albuquerque on 14/04/25.
//

import XCTest

@testable import NyaruDB2

final class BTreeIndexPersistenceTests: XCTestCase {

    func testPersistAndReload() async throws {
        func testPersistedIndexIsEquivalent() async throws {
            let tree = BTreeIndex<String>(minimumDegree: 2)

            // Inserindo alguns dados
            await tree.insert(key: "Alice", data: "Data1".data(using: .utf8)!)
            await tree.insert(key: "Bob", data: "Data2".data(using: .utf8)!)
            await tree.insert(key: "Alice", data: "Data3".data(using: .utf8)!)

            // Persiste o índice em um arquivo temporário
            let fileURL = URL(fileURLWithPath: NSTemporaryDirectory())
                .appendingPathComponent("btreeIndex.dat")
            try await tree.persist(to: fileURL)

            // Cria uma nova BTreeIndex e carrega o estado persistido
            let newTree = BTreeIndex<String>(minimumDegree: 2)
            try await newTree.loadPersistedIndex(from: fileURL)

            // Testa se as buscas são equivalentes
            let originalResult = await tree.search(key: "Alice")
            let loadedResult = await newTree.search(key: "Alice")

            XCTAssertEqual(
                originalResult,
                loadedResult,
                "O índice carregado deve ser equivalente ao original"
            )
        }
    }

}
