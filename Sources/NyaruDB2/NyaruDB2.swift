import Foundation

public class NyaruDB2 {
    public let storage: StorageEngine
    public let indexManager: IndexManager<String>
    private let statsEngine: StatsEngine
    private var collections: [String: NDBCollection] = [:]

    // Parâmetros padrão podem ser estendidos para incluir shardKey, compressão, etc.
    public init(
        path: String = "NyaruDB2",
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) throws {
        self.storage = try StorageEngine(
            path: path,
            compressionMethod: compressionMethod,
            fileProtectionType: fileProtectionType
        )
        self.indexManager = IndexManager()
        self.statsEngine = StatsEngine(storage: storage)
    }
    
    /// Retrieves a previously registered collection.
    public func getCollection(named name: String) -> NDBCollection? {
        return collections[name]
    }

    public func createCollection(name: String, indexes: [String] = [], partitionKey: String) async throws -> NDBCollection {
        // Set the partition key in the StorageEngine for this collection
        await storage.setPartitionKey(for: name, key: partitionKey)
        
        let collection = NDBCollection(storage: storage,
                                       statsEngine: statsEngine,
                                       name: name,
                                       indexes: indexes,
                                       partitionKey: partitionKey)
        collections[name] = collection
        return collection
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

    public func query<T: Codable>(from collection: String) async throws -> Query<T> {
        return Query<T>(
            collection: collection,
            storage: self.storage,
            indexStats: try await self.getIndexStats(),
            shardStats: try await self.getShardStats()
        )
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

    public func getIndexStats() async throws -> [String: IndexStat] {
        return await statsEngine.getIndexStats()
    }
    
    // Returns shard statistics (e.g., document counts and value ranges from metadata).
    public func getShardStats() async throws -> [ShardStat] {
        return try await statsEngine.getShardStats()
    }
}
