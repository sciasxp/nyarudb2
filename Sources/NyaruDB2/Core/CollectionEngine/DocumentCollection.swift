import Foundation

public struct CollectionMetadata: Codable {
    public let name: String
    public let indexes: [String]
    public let partitionKey: String

    public init(name: String, indexes: [String] = [], partitionKey: String) {
        self.name = name
        self.indexes = indexes
        self.partitionKey = partitionKey
    }
}

public class DocumentCollection {
    public let metadata: CollectionMetadata
    private let storage: StorageEngine
    private let statsEngine: StatsEngine

    public init(storage: StorageEngine, statsEngine: StatsEngine, name: String, indexes: [String] = [], partitionKey: String) {
        self.storage = storage
        self.metadata = CollectionMetadata(name: name, indexes: indexes, partitionKey: partitionKey)
        self.statsEngine = statsEngine
    }

    public func insert<T: Codable>(_ document: T) async throws {
        let indexField = metadata.indexes.first
        try await storage.insertDocument(document, collection: metadata.name, indexField: indexField)
    }

    public func bulkInsert<T: Codable>(_ documents: [T]) async throws {
        let indexField = metadata.indexes.first
        try await storage.bulkInsertDocuments(documents, collection: metadata.name, indexField: indexField)
    }

    public func findOne<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String,
        shardValue: String
    ) async throws -> T? {
        return try await _findOne(query: query, shardKey: shardKey, shardValue: shardValue)
    }

    private func _findOne<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String? = nil,
        shardValue: String? = nil
    ) async throws -> T? {
        let results: [T] = try await fetch(query: query, shardKey: shardKey, shardValue: shardValue)
        return results.first
    }

    public func update<T: Codable>(_ document: T, matching predicate: @escaping (T) -> Bool) async throws {
        try await storage.updateDocument(document, in: metadata.name, matching: predicate)
    }

    public func fetch<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String? = nil,
        shardValue: String? = nil
    ) async throws -> [T] {
        // Retrieve all documents from the collection (full scan)
        let results: [T] = try await storage.fetchDocuments(from: metadata.name)

        // Apply shard filtering if specified
        let resultsFilteredByShard: [T] = {
            guard let shardKey = shardKey, let shardValue = shardValue else { return results }
            return results.filter { document in
                guard let data = try? JSONEncoder().encode(document),
                      let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let value = dict[shardKey] as? String
                else { return false }
                return value == shardValue
            }
        }()

        guard let query = query else { return resultsFilteredByShard }

        let finalResults = resultsFilteredByShard.filter { document in
            guard let data = try? JSONEncoder().encode(document),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            else { return false }
            for (key, value) in query {
                if let docValue = dict[key] as? String, let queryValue = value as? String {
                    if docValue != queryValue { return false }
                } else if let docValue = dict[key] as? Int, let queryValue = value as? Int {
                    if docValue != queryValue { return false }
                } else {
                    return false
                }
            }
            return true
        }
        return finalResults
    }

    public func delete<T: Codable>(where predicate: @escaping (T) -> Bool) async throws {
        try await storage.deleteDocuments(where: predicate, from: metadata.name)
    }

    public func query<T: Codable>() async throws -> Query<T> {
    let indexStats = await statsEngine.getIndexStats()  // Using StatsEngine directly.
    let shardStats = try await statsEngine.getShardStats()
    return Query<T>(collection: metadata.name,
                    storage: storage,
                    indexStats: indexStats,
                    shardStats: shardStats)
}


}
