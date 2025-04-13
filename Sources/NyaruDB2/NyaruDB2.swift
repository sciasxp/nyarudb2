import Foundation

public struct NyaruDB2 {
    public let storage: StorageEngine
    public let indexManager: IndexManager
    private let statsEngine: StatsEngine

    // Parâmetros padrão podem ser estendidos para incluir shardKey, compressão, etc.
    public init(
        path: String = "NyaruDB2",
        shardKey: String? = nil,
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) throws {
        self.storage = try StorageEngine(
            path: path,
            shardKey: shardKey,
            compressionMethod: compressionMethod,
            fileProtectionType: fileProtectionType
        )
        self.indexManager = IndexManager()
        self.statsEngine = StatsEngine(storage: storage)
    }
    
    // MARK: - Operações CRUD Simples
    
    /// Insere um documento em uma coleção.
    public func insert<T: Codable>(
        _ document: T,
        into collection: String,
        indexField: String? = nil
    ) async throws {
        try await storage.insertDocument(document, collection: collection, indexField: indexField)
    }
    
    /// Inserção em lote
    public func bulkInsert<T: Codable>(
        _ documents: [T],
        into collection: String,
        indexField: String? = nil
    ) async throws {
        try await storage.bulkInsertDocuments(documents, collection: collection, indexField: indexField)
    }
    
    /// Atualiza um documento com base em um predicado (por exemplo, documento com um determinado "id").
    public func update<T: Codable>(
        _ document: T,
        in collection: String,
        matching predicate: @escaping (T) -> Bool,
        indexField: String? = nil
    ) async throws {
        try await storage.updateDocument(document, in: collection, matching: predicate, indexField: indexField)
    }
    
    /// Deleta documentos de uma coleção que satisfaçam o predicado.
    public func delete<T: Codable>(
        where predicate: @escaping (T) -> Bool,
        from collection: String
    ) async throws {
        try await storage.deleteDocuments(where: predicate, from: collection)
    }
    
    /// Busca todos os documentos de uma coleção.
    public func fetch<T: Codable>(from collection: String) async throws -> [T] {
        return try await storage.fetchDocuments(from: collection)
    }
    
    public func fetchLazy<T: Codable>(from collection: String) async -> AsyncThrowingStream<T, Error> {
        await storage.fetchDocumentsLazy(from: collection)
    }

    public func query<T: Codable>(from collection: String) -> Query<T> {
        return Query<T>(collection: collection)
    }
    
    // MARK: - Operações Administrativas
    
    /// Conta o total de documentos de uma coleção.
    public func countDocuments(in collection: String) async throws -> Int {
        return try await storage.countDocuments(in: collection)
    }
    
    /// Lista as coleções existentes.
    public func listCollections() async throws -> [String] {
        return try await storage.listCollections()
    }
    
    /// Remove completamente uma coleção (drop).
    public func dropCollection(_ collection: String) async throws {
        try await storage.dropCollection(collection)
    }
    
    // Estatísticas
  
    /// Obtém estatísticas detalhadas de uma coleção.
    public func getCollectionStats(for collection: String) async throws -> CollectionStats {
        return try await statsEngine.getCollectionStats(collection)
    }
    
    /// Obtém estatísticas globais do banco (número de coleções, total de documentos, tamanho total).
    public func getGlobalStats() async throws -> GlobalStats {
        return try await statsEngine.getGlobalStats()
    }
}
