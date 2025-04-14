//
//  QueryEngineTests.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 13/04/25.
//

import XCTest
@testable import NyaruDB2

// Modelo para os testes do QueryEngine
struct User: Codable, Equatable {
    let id: Int
    let name: String
    let age: Int
}

final class QueryEngineTests: XCTestCase {
    
    var tempDirectory: URL!
    var storage: StorageEngine!
    
    override func setUp() async throws {
        // Cria um diretório temporário para isolar o teste
        tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        
        // Inicializa o StorageEngine sem particionamento para este teste
        storage = try StorageEngine(
            path: tempDirectory.path,
            shardKey: nil,               // Nenhum particionamento para simplificar
            compressionMethod: .none
        )
        
        // Cria e insere alguns registros na coleção "Users"
        let users: [User] = [
            User(id: 1, name: "Alice", age: 30),
            User(id: 2, name: "Bob", age: 25),
            User(id: 3, name: "Charlie", age: 35),
            User(id: 4, name: "David", age: 40),
            User(id: 5, name: "Alice", age: 45)
        ]
        
        for user in users {
            try await storage.insertDocument(user, collection: "Users")
        }
    }
    
    override func tearDown() async throws {
        // Remove o diretório temporário para limpar o ambiente de teste
        try? FileManager.default.removeItem(at: tempDirectory)
    }
    
    // Helper para coletar os itens de um AsyncThrowingStream em um array.
    private func collect<T>(stream: AsyncThrowingStream<T, Error>) async throws -> [T] {
        var results: [T] = []
        for try await item in stream {
            results.append(item)
        }
        return results
    }
    
    func testQueryEqualOperator() async throws {
        // Query: Filtra usuários cujo nome é "Alice"
        let query = Query<User>(collection: "Users").where("name", .equal("Alice"))
        let results = try await collect(stream: query.fetchStream(from: storage))
        
        // Espera dois registros (com ids 1 e 5)
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results.contains(where: { $0.id == 1 }))
        XCTAssertTrue(results.contains(where: { $0.id == 5 }))
    }
    
    func testQueryGreaterThanOperator() async throws {
        // Query: Filtra usuários com idade maior que 30
        let query = Query<User>(collection: "Users").where("age", .greaterThan(30))
        let results = try await collect(stream: query.fetchStream(from: storage))
        
        // Espera 3 registros: Charlie (35), David (40), Alice (45)
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results.contains(where: { $0.id == 3 }))
        XCTAssertTrue(results.contains(where: { $0.id == 4 }))
        XCTAssertTrue(results.contains(where: { $0.id == 5 }))
    }
    
    func testQueryBetweenOperator() async throws {
        // Query: Filtra usuários com idade entre 30 e 40 inclusive
        let query = Query<User>(collection: "Users").where("age", .between(lower: 30, upper: 40))
        let results = try await collect(stream: query.fetchStream(from: storage))
        
        // Espera: Alice (30), Charlie (35) e David (40)
        XCTAssertEqual(results.count, 3)
        XCTAssertTrue(results.contains(where: { $0.id == 1 }))
        XCTAssertTrue(results.contains(where: { $0.id == 3 }))
        XCTAssertTrue(results.contains(where: { $0.id == 4 }))
    }
    
    func testQueryStartsWithOperator() async throws {
        // Query: Filtra usuários cujo nome começa com "A"
        let query = Query<User>(collection: "Users").where("name", .startsWith("A"))
        let results = try await collect(stream: query.fetchStream(from: storage))
        
        // Espera: Usuários "Alice" (ids 1 e 5)
        XCTAssertEqual(results.count, 2)
        XCTAssertTrue(results.contains(where: { $0.id == 1 }))
        XCTAssertTrue(results.contains(where: { $0.id == 5 }))
    }
    
    func testQueryContainsOperator() async throws {
        // Query: Filtra usuários cujo nome contenha a letra "v" (deve pegar "David")
        let query = Query<User>(collection: "Users").where("name", .contains("v"))
        let results = try await collect(stream: query.fetchStream(from: storage))
        
        XCTAssertEqual(results.count, 1)
        XCTAssertEqual(results.first?.id, 4)
    }
    
    func testFetchStreamFiltering() async throws {
            // Cria um diretório temporário para isolar o teste
            let tempDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
            
            // Inicializa o StorageEngine sem particionamento para simplificar
            let storage = try StorageEngine(
                path: tempDirectory.path,
                shardKey: nil,
                compressionMethod: .none,
                fileProtectionType: .none
            )
            
            // Insere alguns documentos
            let model1 = User(id: 1, name: "Alice", age: 30)
            let model2 = User(id: 2, name: "Bob", age: 25)
            let model3 = User(id: 3, name: "Charlie", age: 30)
            try await storage.insertDocument(model1, collection: "People")
            try await storage.insertDocument(model2, collection: "People")
            try await storage.insertDocument(model3, collection: "People")
            
            // Cria a query para filtrar onde a idade é igual a 30
            var query = Query<User>(collection: "People")
            query = query.where("age", .equal(30))
            
            // Executa o fetchStream para recuperar os documentos que atendem ao predicado
            let stream = query.fetchStream(from: storage)
            var results: [User] = []
            for try await person in stream {
                results.append(person)
            }
            
            // Espera-se que apenas os documentos com age == 30 sejam retornados (model1 e model3)
            XCTAssertEqual(results.count, 2)
            XCTAssertTrue(results.contains(model1))
            XCTAssertTrue(results.contains(model3))
        }
}
