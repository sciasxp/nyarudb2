import Foundation


/// A generic structure representing a query for a specific type of data.
/// 
/// This structure allows the creation and execution of queries on a collection
/// of data, supporting predicates and key path-based filtering.
///
/// - Parameters:
///   - T: The type of data being queried, which must conform to `Codable`.
///
/// Properties:
/// - `collection`: The name of the collection being queried.
/// - `predicates`: A list of field-based predicates used to filter the query results.
/// - `storage`: The storage engine responsible for managing the underlying data.
/// - `planner`: The query planner responsible for optimizing and executing the query.
/// - `keyPathPredicates`: A list of key path-based predicates used for filtering the query results.
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
    ///   - indexStats: A dictionary containing statistics for each index, where the key is the index name and the value is its corresponding `IndexStat`.
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

    
    /// Adds a filtering condition to the query based on the specified key path and query operator.
    /// 
    /// - Parameters:
    ///   - keyPath: A key path to the property of the model `T` that will be used for filtering.
    ///   - op: The `QueryOperator` that defines the condition to apply to the property.
    /// 
    /// - Note: This method modifies the current query by appending the specified condition.
    public mutating func `where`<V: Hashable>(
        _ keyPath: KeyPath<T, V>,
        _ op: QueryOperator
    ) {
        let predicate: (T) -> AnyHashable = { doc in
            return doc[keyPath: keyPath] as AnyHashable
        }
        keyPathPredicates.append((predicate, op))
    }

    
    /// Provides a detailed execution plan for the current query.
    ///
    /// - Returns: An `ExecutionPlan` object that describes the steps and operations
    ///   involved in executing the query. This can be used for debugging or
    ///   optimizing query performance.
    public func explain() -> ExecutionPlan {
        let availableIndexes = Array(planner.indexStats.keys)
        return planner.optimize(
            collection: collection,
            predicates: predicates,
            availableIndexes: availableIndexes
        )
    }

    
    /// Executes the query and returns an array of results of type `T`.
    ///
    /// This function performs the query asynchronously and may throw an error
    /// if the execution fails.
    ///
    /// - Returns: An array of results of type `T`.
    /// - Throws: An error if the query execution fails.
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

    /// Executes the query plan using only the index, without accessing the full data records.
    ///
    /// - Parameter plan: The execution plan that defines the query to be executed.
    /// - Returns: An array of results of type `T` that match the query criteria.
    /// - Throws: An error if the execution of the query fails.
    /// - Note: This method is asynchronous and leverages the index for efficient query execution.
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

    /// Executes a hybrid query plan asynchronously and returns the results.
    ///
    /// This method processes the provided execution plan, combining different
    /// query strategies to retrieve the desired data. It is designed to handle
    /// complex queries that may involve multiple steps or data sources.
    ///
    /// - Parameter plan: The `ExecutionPlan` object that defines the query strategy
    ///   and steps to be executed.
    /// - Returns: An array of results of type `T` that match the query criteria.
    /// - Throws: An error if the execution of the query plan fails.
    /// - Note: This method is asynchronous and must be called with `await`.
    private func executeHybrid(plan: ExecutionPlan) async throws -> [T] {
        
        let allShards = try await storage.getShardManagers(for: collection)

        
        let shards = allShards.filter { shard in
            return shard.metadata.documentCount > 0
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

    /// Executes a full scan of the database based on the provided execution plan.
    ///
    /// This method performs a comprehensive scan of all records in the database
    /// to retrieve results that match the criteria specified in the execution plan.
    ///
    /// - Parameter plan: The `ExecutionPlan` object that defines the criteria and
    ///   conditions for the scan.
    /// - Returns: An array of objects of type `T` that match the execution plan.
    /// - Throws: An error if the scan fails or encounters an issue during execution.
    /// - Note: This operation may be resource-intensive as it scans all records
    ///   in the database. Use with caution for large datasets.
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
    ///   - op: The `QueryOperator` that defines the evaluation logic.
    /// - Returns: A Boolean indicating whether the value satisfies the condition defined by the query operator.
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
    ///   - value1: The first value to compare. This value is optional and can be `nil`.
    ///   - value2: The second value to compare. This value is non-optional.
    /// - Returns: A Boolean value indicating whether the two values are considered equal.
        if let s1 = stringValue(value1), let s2 = stringValue(value2) {
            return s1 == s2
        }
        return false
    }

    /// Converts a given value to a `Double` if possible.
    ///
    /// - Parameter value: The value to be converted, which can be of any type.
    /// - Returns: A `Double` representation of the value if the conversion is successful, or `nil` if the value cannot be converted.
    private func toDouble(_ value: Any?) -> Double? {
        if let num = value as? NSNumber { return num.doubleValue }
        if let str = value as? String, let d = Double(str) { return d }
        if let d = value as? Double { return d }
        if let i = value as? Int { return Double(i) }
        return nil
    }

    /// Converts the given value to a `String` if possible.
    ///
    /// - Parameter value: The value to be converted, which can be of any type or `nil`.
    /// - Returns: A `String` representation of the value if it can be converted, or `nil` if the conversion is not possible.
    private func stringValue(_ value: Any?) -> String? {
        if let v = value { return "\(v)" }
        return nil
    }

    /// Compares two numeric values based on a specified condition.
    ///
    /// This method is used to evaluate numeric comparisons within the query engine.
    /// It takes two numeric values and a comparison operator, and determines whether
    /// the condition is satisfied.
    ///
    /// - Parameters:
    ///   - lhs: The left-hand side numeric value to compare.
    ///   - rhs: The right-hand side numeric value to compare.
    ///   - condition: The comparison operator to apply (e.g., `==`, `<`, `>`, etc.).
    /// - Returns: A Boolean value indicating whether the comparison condition is satisfied.
    /// - Note: This method assumes that both `lhs` and `rhs` are valid numeric types.
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
