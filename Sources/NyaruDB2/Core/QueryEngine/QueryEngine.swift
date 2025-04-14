import Foundation

// This is the refactored QueryEngine that now integrates with the QueryPlanner.
public struct Query<T: Codable> {
    private let collection: String
    private var predicates: [(field: String, op: QueryOperator)] = []
    private let storage: StorageEngine
    private let planner: QueryPlanner

    // Initialize the query with storage and statistics for planning.
    public init(
        collection: String,
        storage: StorageEngine,
        indexStats: [String: IndexStat],
        shardStats: [ShardStat]
    ) {
        self.collection = collection
        self.storage = storage
        self.planner = QueryPlanner(
            indexStats: indexStats,
            shardStats: shardStats
        )
    }

    /// Adds a predicate to the query.
    public mutating func `where`(_ field: String, _ op: QueryOperator) {
        predicates.append((field: field, op: op))
    }

    /// Returns an execution plan for debugging or optimization purposes.
    public func explain() -> ExecutionPlan {
        let availableIndexes = Array(planner.indexStats.keys)
        return planner.optimize(
            collection: collection,
            predicates: predicates,
            availableIndexes: availableIndexes
        )
    }

    /// Executes the query based on the execution plan.
    /// Currently, this implementation always uses a full scan but could be extended to support index-only and hybrid scans.
    public func execute() async throws -> [T] {
        // For now, use a full scan strategy.
        var results = [T]()
        let shards = try await storage.getShardManagers(for: collection)

        for shard in shards {
            let docs: [T] = try await shard.loadDocuments()
            for doc in docs {
                let dict = try convertToDictionary(doc)
                var match = true
                for (field, op) in predicates {
                    let fieldValue = dict[field]
                    if !evaluatePredicate(documentValue: fieldValue, op: op) {
                        match = false
                        break
                    }
                }
                if match {
                    results.append(doc)
                }
            }
        }
        return results
    }
    
    public func fetchStream(from storage: StorageEngine) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    // Obtém os shards da coleção
                    let shards = try await storage.getShardManagers(for: collection)
                    
                    // Para cada shard, itera via o método lazy (supondo que shard.loadDocumentsLazy retorna AsyncThrowingStream)
                    for shard in shards {
                        for try await doc in shard.loadDocumentsLazy() as AsyncThrowingStream<T, Error> {
                            let dict = try convertToDictionary(doc)
                            var satisfies = true
                            for (field, op) in predicates {
                                let fieldValue = dict[field]
                                if !evaluatePredicate(documentValue: fieldValue, op: op) {
                                    satisfies = false
                                    break
                                }
                            }
                            if satisfies {
                                continuation.yield(doc)
                            }
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }
    

    // MARK: - Helpers

    private func convertToDictionary(_ value: T) throws -> [String: Any] {
        let data = try JSONEncoder().encode(value)
        let obj = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = obj as? [String: Any] else {
            throw NSError(
                domain: "QueryEngine",
                code: 0,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Failed to convert object to dictionary"
                ]
            )
        }
        return dict
    }

    private func evaluatePredicate(documentValue: Any?, op: QueryOperator)
        -> Bool
    {
        switch op {
        case .exists:
            return documentValue != nil
        case .notExists:
            return documentValue == nil
        case .equal(let target):
            return compareEquality(documentValue, target)
        case .notEqual(let target):
            return !compareEquality(documentValue, target)
        case .lessThan(let target):
            return compareNumeric(documentValue, target, using: <)
        case .lessThanOrEqual(let target):
            return compareNumeric(documentValue, target, using: <=)
        case .greaterThan(let target):
            return compareNumeric(documentValue, target, using: >)
        case .greaterThanOrEqual(let target):
            return compareNumeric(documentValue, target, using: >=)
        case .between(let lower, let upper):
            return compareNumeric(documentValue, lower, using: >=)
                && compareNumeric(documentValue, upper, using: <=)
        case .contains(let substring):
            if let s1 = stringValue(documentValue),
                let s2 = stringValue(substring)
            {
                return s1.contains(s2)
            }
            return false
        case .startsWith(let prefix):
            if let s1 = stringValue(documentValue) {
                return s1.hasPrefix(prefix)
            }
            return false
        case .endsWith(let suffix):
            if let s1 = stringValue(documentValue) {
                return s1.hasSuffix(suffix)
            }
            return false
        default:
            return false
        }
    }

    private func compareEquality(_ value1: Any?, _ value2: Any) -> Bool {
        if let d1 = toDouble(value1), let d2 = toDouble(value2) {
            return d1 == d2
        }
        if let s1 = stringValue(value1), let s2 = stringValue(value2) {
            return s1 == s2
        }
        return false
    }

    private func toDouble(_ value: Any?) -> Double? {
        if let num = value as? NSNumber { return num.doubleValue }
        if let str = value as? String, let d = Double(str) { return d }
        if let d = value as? Double { return d }
        if let i = value as? Int { return Double(i) }
        return nil
    }

    private func stringValue(_ value: Any?) -> String? {
        if let v = value { return "\(v)" }
        return nil
    }

    private func compareNumeric(
        _ value1: Any?,
        _ value2: Any,
        using comparator: (Double, Double) -> Bool
    ) -> Bool {
        if let d1 = toDouble(value1), let d2 = toDouble(value2) {
            return comparator(d1, d2)
        }
        if let s1 = stringValue(value1), let s2 = stringValue(value2) {
            return comparator(Double(s1.hashValue), Double(s2.hashValue))
        }
        return false
    }
}
