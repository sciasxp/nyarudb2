import Foundation


/// A generic structure representing a query for a specific type of data.
/// 
/// This structure is used to build and execute queries on a collection of data
/// stored in a database. It supports filtering data using predicates and key path
/// predicates, and relies on a storage engine and query planner for execution.
/// 
/// - Parameters:
///   - T: The type of the data being queried. Must conform to `Codable`.
///
/// - Properties:
///   - collection: The name of the collection being queried.
///   - predicates: A list of field-based predicates used to filter the data.
///   - storage: The storage engine responsible for managing the data.
///   - planner: The query planner responsible for optimizing and executing the query.
///   - keyPathPredicates: A list of key path-based predicates used to filter the data.
public struct Query<T: Codable> {
    private let collection: String
    private var predicates: [(field: String, op: QueryOperator)] = []
    private let storage: StorageEngine
    private let planner: QueryPlanner
    private var keyPathPredicates: [((T) -> AnyHashable, QueryOperator)] = []

    
    /// Initializes a new instance of the `QueryEngine`.
    ///
    /// - Parameters:
    ///   - collection: The name of the collection to be queried.
    ///   - storage: The storage engine responsible for managing data persistence.
    ///   - indexStats: A dictionary containing statistics for each index, where the key is the index name and the value is an `IndexStat` object.
    ///   - shardStats: An array of `ShardStat` objects representing statistics for each shard.
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

    
    /// Filters the query results based on the specified key path and query operator.
    /// 
    /// - Parameters:
    ///   - keyPath: A key path to the property of the model `T` that will be used for filtering.
    ///   - op: The query operator that defines the condition to apply to the specified key path.
    /// 
    /// - Note: This method is mutating, meaning it modifies the state of the query engine.
    public mutating func `where`<V: Hashable>(
        _ keyPath: KeyPath<T, V>,
        _ op: QueryOperator
    ) {
        let predicate: (T) -> AnyHashable = { doc in
            return doc[keyPath: keyPath] as AnyHashable
        }
        keyPathPredicates.append((predicate, op))
    }

    
    /// Generates and returns an execution plan for the current query.
    ///
    /// This method provides a detailed explanation of how the query will be executed,
    /// including information about the steps involved and any optimizations applied.
    ///
    /// - Returns: An `ExecutionPlan` object representing the detailed execution strategy
    ///   for the query.
    public func explain() -> ExecutionPlan {
        let availableIndexes = Array(planner.indexStats.keys)
        return planner.optimize(
            collection: collection,
            predicates: predicates,
            availableIndexes: availableIndexes
        )
    }

    
    /// Executes the query asynchronously and returns an array of results.
    ///
    /// - Returns: An array of results of type `T`.
    /// - Throws: An error if the query execution fails.
    /// - Note: This method is asynchronous and must be awaited.
    public func execute() async throws -> [T] {
        let plan = explain()

        switch plan.indexStrategy {
        case .indexOnly:
            return try await executeIndexOnly(plan: plan)
        case .hybrid:
            return try await executeHybrid(plan: plan)
        case .fullScan:
            return try await executeFullScan(plan: plan)
        }
    }

    /// Executes a query plan that only involves indexed fields.
    ///
    /// This method processes the provided execution plan using only the indexed fields
    /// to retrieve the results. It is designed for efficient query execution when the
    /// query can be satisfied entirely using indexes, without needing to scan the full dataset.
    ///
    /// - Parameter plan: The execution plan that specifies the query to be executed.
    /// - Returns: An array of results of type `T` that match the query criteria.
    /// - Throws: An error if the execution of the query fails.
    /// - Note: This method is asynchronous and must be called with `await`.
    private func executeIndexOnly(plan: ExecutionPlan) async throws -> [T] {
        
        guard let indexField = plan.usedIndex else { return [] }

        let values =
            predicates
            .filter { $0.field == indexField }
            .compactMap { predicate -> AnyHashable? in
                if case .equal(let value) = predicate.op {
                    return value
                }
                return nil
            }

        // Usa um TaskGroup para executar a busca para cada valor do índice em paralelo
        return try await withThrowingTaskGroup(of: [T].self) { group in
            for value in values {
                group.addTask {
                    try await self.storage.fetchFromIndex(
                        collection: self.collection,
                        field: indexField,
                        value: value as! String
                    )
                }
            }
            return try await group.reduce(into: []) { $0 += $1 }
        }
    }

    /// Executes a hybrid query execution plan asynchronously.
    ///
    /// This method processes the provided execution plan and returns the results
    /// as an array of type `T`. It combines multiple query execution strategies
    /// to optimize performance and resource usage.
    ///
    /// - Parameter plan: The execution plan to be processed.
    /// - Returns: An array of results of type `T` obtained from executing the plan.
    /// - Throws: An error if the execution fails.
    /// - Note: This method is asynchronous and must be called with `await`.
    private func executeHybrid(plan: ExecutionPlan) async throws -> [T] {
        
        let allShards = try await storage.getShardManagers(for: collection)

        
        let shards = allShards.filter { shard in
            return shard.metadata.documentCount > 0
            // TODO: return shard.metadata.toShardStat().matches(any: predicates)
        }

        
        return try await withThrowingTaskGroup(of: [T].self) { group in
            for shard in shards {
                group.addTask {
                    let docs: [T] = try await shard.loadDocuments()
                    
                    return docs.filter { self.evaluateDocument($0) }
                }
            }
            var results = [T]()
            for try await shardResults in group {
                results.append(contentsOf: shardResults)
            }
            return results
        }
    }

    /// Executes a full scan on the database based on the provided execution plan.
    ///
    /// This method performs a complete traversal of the database to retrieve all records
    /// that match the criteria specified in the execution plan. It is typically used when
    /// no indexes are available or applicable for the query.
    ///
    /// - Parameter plan: The execution plan containing the criteria and instructions for the query.
    /// - Returns: An array of results of type `T` that match the query criteria.
    /// - Throws: An error if the execution of the full scan fails.
    /// - Note: This operation may be resource-intensive for large datasets as it scans all records.
    private func executeFullScan(plan: ExecutionPlan) async throws -> [T] {
        var results = [T]()

        let stream: AsyncThrowingStream<T, Error> =
            await storage.fetchDocumentsLazy(from: collection)
        for try await doc in stream {
            if self.evaluateDocument(doc) {
                results.append(doc)
            }
        }

        return results
    }

    /// Evaluates whether a given document satisfies certain conditions.
    ///
    /// - Parameter doc: The document of generic type `T` to be evaluated.
    /// - Returns: A Boolean value indicating whether the document meets the evaluation criteria.
    internal func evaluateDocument(_ doc: T) -> Bool {
        return keyPathPredicates.allSatisfy { (predicate, op) in
            evaluateValue(predicate(doc), op: op)
        }
    }

    /// Evaluates a given value against a specified query operator.
    ///
    /// - Parameters:
    ///   - value: The value to be evaluated. Must conform to `AnyHashable`.
    ///   - op: The `QueryOperator` that defines the evaluation criteria.
    /// - Returns: A Boolean indicating whether the value satisfies the query operator.
    private func evaluateValue(_ value: AnyHashable, op: QueryOperator) -> Bool
    {
        switch op {
        case .equal(let target):
            return value == target
        case .notEqual(let target):
            return value != target
        case .range(let lower, let upper),
            .between(let lower, let upper):
            if let v = value as? Int, let l = lower as? Int,
                let u = upper as? Int
            {
                return v >= l && v <= u
            }
            if let v = value as? Double, let l = lower as? Double,
                let u = upper as? Double
            {
                return v >= l && v <= u
            }
            return false
        case .greaterThan(let target):
            if let v = value as? Int, let t = target as? Int {
                return v > t
            }
            if let v = value as? Double, let t = target as? Double {
                return v > t
            }
            return false
        case .greaterThanOrEqual(let target):
            if let v = value as? Int, let t = target as? Int {
                return v >= t
            }
            if let v = value as? Double, let t = target as? Double {
                return v >= t
            }
            return false
        case .lessThan(let target):
            if let v = value as? Int, let t = target as? Int {
                return v < t
            }
            if let v = value as? Double, let t = target as? Double {
                return v < t
            }
            return false
        case .lessThanOrEqual(let target):
            if let v = value as? Int, let t = target as? Int {
                return v <= t
            }
            if let v = value as? Double, let t = target as? Double {
                return v <= t
            }
            return false
        case .contains(let substring):
            if let s = value as? String {
                return s.contains(substring)
            }
            return false
        case .startsWith(let prefix):
            if let s = value as? String {
                return s.hasPrefix(prefix)
            }
            return false
        case .endsWith(let suffix):
            if let s = value as? String {
                return s.hasSuffix(suffix)
            }
            return false

        case .in(let values):
            for candidate in values {
                if let candidateHashable = candidate as? AnyHashable,
                    candidateHashable == value
                {
                    return true
                }
            }
            return false
        default:
            return false
        }

    }

    /// Compares two values for equality.
    ///
    /// - Parameters:
    ///   - a: The first value to compare. This can be of any type.
    ///   - b: The second value to compare, which must conform to `AnyHashable`.
    /// - Returns: A Boolean value indicating whether the two values are considered equal.
    private func isEqual(_ a: Any, _ b: AnyHashable) -> Bool {
        guard let aHashable = a as? AnyHashable else { return false }
        return aHashable == b
    }

    /// Determines whether a given value is within a specified range.
    ///
    /// - Parameters:
    ///   - value: The value to check.
    ///   - lower: The lower bound of the range, inclusive.
    ///   - upper: The upper bound of the range, inclusive.
    /// - Returns: A Boolean value indicating whether the value is within the range.
    private func isInRange(_ value: Any, lower: AnyHashable, upper: AnyHashable)
        -> Bool
    {
        
        if let dValue = toDouble(value), let dLower = toDouble(lower),
            let dUpper = toDouble(upper)
        {
            return dValue >= dLower && dValue <= dUpper
        }

        
        if let sValue = stringValue(value), let sLower = stringValue(lower),
            let sUpper = stringValue(upper)
        {
            return sValue >= sLower && sValue <= sUpper
        }

        return false
    }

    /// Fetches a stream of elements from the specified storage engine.
    ///
    /// This method returns an `AsyncThrowingStream` that allows asynchronous iteration
    /// over elements of type `T` retrieved from the provided `StorageEngine`.
    ///
    /// - Parameter storage: The `StorageEngine` instance from which the elements will be fetched.
    /// - Returns: An `AsyncThrowingStream` of type `T` that can throw an error during iteration.
    /// - Throws: An error if the stream encounters an issue during fetching.
    public func fetchStream(from storage: StorageEngine) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    // Obtém os shards da coleção
                    let shards = try await storage.getShardManagers(
                        for: collection
                    )
                    // Itera sobre os shards
                    for shard in shards {
                        // Para cada documento do shard, via lazy stream
                        for try await doc in shard.loadDocumentsLazy()
                            as AsyncThrowingStream<T, Error>
                        {
                            // Usa nossa função evaluateDocument que acessa os keyPathPredicates
                            if self.evaluateDocument(doc) {
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

    
    /// Converts a value of type `T` into a dictionary representation.
    ///
    /// - Parameter value: The value of type `T` to be converted.
    /// - Returns: A dictionary where the keys are `String` and the values are `Any`, representing the converted value.
    /// - Throws: An error if the conversion fails.
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

    /// Evaluates a predicate by comparing a document value with a specified query operator.
    ///
    /// - Parameters:
    ///   - documentValue: The value from the document to be evaluated. This can be of any type.
    ///   - op: The query operator that defines the condition to evaluate against the document value.
    /// - Returns: A Boolean value indicating whether the document value satisfies the condition defined by the query operator.
    private func evaluatePredicate(documentValue: Any?, op: QueryOperator) -> Bool {
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
