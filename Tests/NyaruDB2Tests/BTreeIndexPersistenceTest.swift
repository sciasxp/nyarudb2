//
//  BTreeIndexPersistenceTest.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 14/04/25.
//

import XCTest
@testable import NyaruDB2

final class BTreeIndexPersistenceTests: XCTestCase {

    func testPersistAndReload() async throws {
        // Create a BTreeIndex instance and insert some keys.
        let tree = BTreeIndex<String>(minimumDegree: 2)
        await tree.insert(key: "apple", data: "fruit".data(using: .utf8)!)
        await tree.insert(key: "carrot", data: "vegetable".data(using: .utf8)!)
        await tree.insert(key: "banana", data: "fruit".data(using: .utf8)!)

        // Persist the tree to a temporary file.
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("btree_test.json")
        try await tree.persist(to: tempURL)
        
        // Reload the tree from the persisted state.
        let reloadedTree = BTreeIndex<String>(minimumDegree: 2)
        
        // Compare the in-order traversals.
        let originalInOrder = await tree.inOrder()
        let reloadedInOrder = await reloadedTree.inOrder()
        
        XCTAssertEqual(originalInOrder, reloadedInOrder, "The reloaded tree should match the original tree.")
    }
}
