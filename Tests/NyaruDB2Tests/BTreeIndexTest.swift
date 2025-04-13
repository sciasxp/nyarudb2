//
//  BTreeIndexTest.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 11/04/25.
//

import XCTest
@testable import NyaruDB2

final class BTreeIndexTests: XCTestCase {
    
    func testInsertionAndSearch() async {
        // Cria uma B-Tree com chaves do tipo String
        let index = BTreeIndex<String>(minimumDegree: 2)
        let data1 = "Data1".data(using: .utf8)!
        let data2 = "Data2".data(using: .utf8)!
        let key = "key1"
        
        // Insere dois registros com a mesma chave
        await index.insert(key: key, data: data1)
        await index.insert(key: key, data: data2)
        
        // Pesquisa pela chave e verifica se ambos os dados são retornados
        let result = await index.search(key: key)
        XCTAssertNotNil(result)
        XCTAssertEqual(result?.count, 2)
        XCTAssertTrue(result!.contains(data1))
        XCTAssertTrue(result!.contains(data2))
    }
    
    func testSearchNonExistentKey() async {
        let index = BTreeIndex<String>(minimumDegree: 2)
        let result = await index.search(key: "nonexistent")
        XCTAssertNil(result)
    }
}
