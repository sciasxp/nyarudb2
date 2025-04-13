import Foundation

public actor StorageEngine {
    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    private let shardKey: String?
    private var activeShardManagers = [String: ShardManager]()
    private var indexManagers = [String: IndexManager]()
    
    public enum StorageError: Error {
        case invalidDocument
        case partitionKeyNotFound(String)
        case indexKeyNotFound(String)
        case shardManagerCreationFailed
    }

    public init(
        path: String,
        shardKey: String? = nil,
        compressionMethod: CompressionMethod = .none
    ) throws {
        self.baseURL = URL(fileURLWithPath: path, isDirectory: true)
        self.shardKey = shardKey
        self.compressionMethod = compressionMethod
        
        try FileManager.default.createDirectory(
            at: baseURL,
            withIntermediateDirectories: true
        )
    }

    public func insertDocument<T: Codable>(
        _ document: T,
        collection: String,
        indexField: String? = nil
    ) async throws {
        let jsonData = try JSONEncoder().encode(document)
        
        // 1. Otimização: Decodificação parcial apenas para chaves necessárias
        let partitionValue = try shardKey.flatMap { key in
            try extractValue(from: jsonData, key: key)
        }
        
        // 2. Gerenciamento eficiente de shards
        let shardManager = try await getOrCreateShardManager(for: collection)
        let shard = try await shardManager.getOrCreateShard(id: partitionValue ?? "default")
        
        // 3. Operações de I/O assíncronas
        try await shard.appendDocument(document, jsonData: jsonData)
        
        // 4. Indexação otimizada
        if let indexField = indexField {
            try await updateIndex(
                collection: collection,
                indexField: indexField,
                jsonData: jsonData
            )
        }
    }

    // MARK: - Métodos auxiliares
    private func extractValue(from data: Data, key: String) throws -> String {
        do {
            let container = try JSONDecoder().decode([String: String].self, from: data)
            if let value = container[key] {
                return value
            } else {
                throw StorageError.partitionKeyNotFound(key)
            }
        } catch {
            throw StorageError.partitionKeyNotFound(key)
        }
    }

    private func getOrCreateShardManager(for collection: String) async throws -> ShardManager {
        if let existing = activeShardManagers[collection] {
            return existing
        }
        
        let collectionURL = baseURL.appendingPathComponent(collection, isDirectory: true)
        try FileManager.default.createDirectory(at: collectionURL, withIntermediateDirectories: true)
        
        let newManager = ShardManager(
            baseURL: collectionURL,
            compressionMethod: compressionMethod
        )
        activeShardManagers[collection] = newManager
        return newManager
    }

    private func updateIndex(
        collection: String,
        indexField: String,
        jsonData: Data
    ) async throws {
        let indexManager = indexManagers[collection] ?? {
            let newManager = IndexManager()
            newManager.createIndex(for: indexField)
            indexManagers[collection] = newManager
            return newManager
        }()
        
        let dict = try JSONDecoder().decode([String: String].self, from: jsonData)
        guard let key = dict[indexField] else { throw StorageError.indexKeyNotFound(indexField) }
        indexManager.insert(index: indexField, key: key, data: jsonData)
    }
}

// MARK: - Extensões auxiliares
private struct DynamicCodingKey: CodingKey {
    var stringValue: String
    init?(stringValue: String) {
        self.stringValue = stringValue
    }
    
    var intValue: Int? { nil }
    init?(intValue: Int) { nil }
}

