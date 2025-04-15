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

public class NDBCollection {
    public let metadata: CollectionMetadata
    private let db: NyaruDB2

    public init(db: NyaruDB2, name: String, indexes: [String] = [], partitionKey: String) {
        self.db = db
        self.metadata = CollectionMetadata(name: name, indexes: indexes, partitionKey: partitionKey)
    }

    public func insert<T: Codable>(_ document: T) async throws {

        let indexField = metadata.indexes.first
        try await db.insert(document, into: metadata.name, indexField: indexField)
    }

    public func bulkInsert<T: Codable>(_ documents: [T]) async throws {
        let indexField = metadata.indexes.first
        try await db.bulkInsert(documents, into: metadata.name, indexField: indexField)
    }

   public func findOne<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String,
        shardValue: String
    ) async throws -> T? {
        return try await _findOne(query: query, shardKey: shardKey, shardValue: shardValue)
    }

    internal func _findOne<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String? = nil,
        shardValue: String? = nil
    ) async throws -> T? {
        let results: [T] = try await fetch(query: query, shardKey: shardKey, shardValue: shardValue)
        return results.first
    }
    
    public func update<T: Codable>(_ document: T, matching predicate: @escaping (T) -> Bool) async throws {
        try await db.update(document, in: metadata.name, matching: predicate)
    }

    public func fetch<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String? = nil,
        shardValue: String? = nil
    ) async throws -> [T] {
        // Retrieve all documents from the collection (full scan).
        let results: [T] = try await db.fetch(from: metadata.name)
        
        // Apply shard filtering if specified.
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
        
        // If no additional query filtering is required, return the (possibly shard-filtered) results.
        guard let query = query else { return resultsFilteredByShard }
        
        // Further filter by the additional query predicates.
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
        try await db.delete(where: predicate, from: metadata.name)
    }
}
