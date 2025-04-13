import Foundation

public actor StorageEngine {

    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    private let fileProtectionType: FileProtectionType
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
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) throws {
        self.baseURL = URL(fileURLWithPath: path, isDirectory: true)
        self.shardKey = shardKey
        self.compressionMethod = compressionMethod
        self.fileProtectionType = fileProtectionType

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
        let shard = try await shardManager.getOrCreateShard(
            id: partitionValue ?? "default"
        )

        // 3. Operações de I/O assíncronas
        try await shard.appendDocument(document, jsonData: jsonData)

        // 4. Indexação otimizada
        if let indexField = indexField {
            let key = try extractValue(from: jsonData, key: indexField, forIndex: true)

            let indexManager = indexManagers[
                collection,
                default: IndexManager()
            ]
            indexManager.createIndex(for: indexField)
            indexManager.insert(index: indexField, key: key, data: jsonData)
            indexManagers[collection] = indexManager
        }
    }

    // MARK: - Métodos auxiliares
    private func extractValue(
        from data: Data,
        key: String,
        forIndex: Bool = false
    ) throws -> String {
        // Converte os dados para um dicionário genérico
        guard
            let jsonObject = try JSONSerialization.jsonObject(
                with: data,
                options: []
            ) as? [String: Any]
        else {
            throw StorageError.invalidDocument
        }
        // Se não encontrar a chave, lança o erro apropriado
        guard let value = jsonObject[key] else {
            if forIndex {
                throw StorageError.indexKeyNotFound(key)
            }
            throw StorageError.partitionKeyNotFound(key)
        }
        // Converte o valor para String, independente do tipo original
        return String(describing: value)
    }

    private func getOrCreateShardManager(for collection: String) async throws
        -> ShardManager
    {
        if let existing = activeShardManagers[collection] {
            return existing
        }

        let collectionURL = baseURL.appendingPathComponent(
            collection,
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: collectionURL,
            withIntermediateDirectories: true
        )

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
        let indexManager =
            indexManagers[collection]
            ?? {
                let newManager = IndexManager()
                newManager.createIndex(for: indexField)
                indexManagers[collection] = newManager
                return newManager
            }()

        let key = try extractValue(from: jsonData, key: indexField, forIndex: true)
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
