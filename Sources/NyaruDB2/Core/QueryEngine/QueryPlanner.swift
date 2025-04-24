import Foundation

// MARK: - ExecutionPlan

/// A structure representing the execution plan for a query in the database.
///
/// The `ExecutionPlan` provides details about how a query will be executed,
/// including the collection being queried, the predicates applied, the estimated
/// number of documents to scan, and the index strategy used.
///
/// - Properties:
///   - collection: The name of the collection being queried.
///   - predicates: An array of tuples representing the query predicates, where each
///     tuple contains a field name and a query operator.
///   - estimatedDocsToScan: The estimated number of documents that will be scanned
///     during the query execution.
///   - shardsToSkip: The number of shards that can be skipped during query execution.
///   - usedIndex: The name of the index used for the query, if any. If no index is used,
///     this will be `nil`.
///   - indexStrategy: The strategy used for indexing during query execution.
///
/// - Conforms to:
///   - `CustomStringConvertible`: Provides a human-readable description of the execution plan.
public struct ExecutionPlan: CustomStringConvertible {
    public let collection: String
    public let predicates: [(field: String, op: QueryOperator)]
    public let estimatedDocsToScan: Int
    public let shardsToSkip: Int
    public let usedIndex: String?
    public let indexStrategy: IndexStrategy

    public var description: String {
        """
        ExecutionPlan for \(collection):
        - Predicates: \(predicates.map { "\($0.field) \($0.op)" }.joined(separator: ", "))
        - Estimated docs to scan: \(estimatedDocsToScan)
        - Shards to skip: \(shardsToSkip)
        - Used index: \(usedIndex ?? "none") (\(indexStrategy))
        """
    }

    /// An enumeration that defines the strategies for querying data in the database.
    ///
    /// - `fullScan`: Represents a strategy where the entire dataset is scanned to find matching results.
    /// - `indexOnly`: Represents a strategy where only the index is used to retrieve results, without scanning the full dataset.
    /// - `hybrid`: Represents a strategy that combines both index usage and partial dataset scanning to optimize query performance.
    public enum IndexStrategy: CustomStringConvertible {
        case fullScan
        case indexOnly
        case hybrid

        /// A textual representation of the query execution strategy.
        /// 
        /// - Returns: A `String` describing the query execution strategy:
        ///   - `"Full Scan"`: Indicates that the query will perform a full scan of the data.
        ///   - `"Index Only"`: Indicates that the query will use only the index for execution.
        ///   - `"Hybrid"`: Indicates that the query will use a combination of index and data scanning.
        public var description: String {
            switch self {
            case .fullScan: return "Full Scan"
            case .indexOnly: return "Index Only"
            case .hybrid: return "Hybrid"
            }
        }
    }
}

// MARK: - Query Operator

/// An enumeration representing various query operators that can be used to filter or match data in a query.
///
/// Each case represents a specific type of operation that can be performed on the data.
///
/// - Cases:
///   - `equal`: Matches values that are equal to the specified value.
///   - `notEqual`: Matches values that are not equal to the specified value.
///   - `range`: Matches values that fall within the specified lower and upper bounds.
///   - `contains`: Matches strings that contain the specified substring.
///   - `startsWith`: Matches strings that start with the specified prefix.
///   - `endsWith`: Matches strings that end with the specified suffix.
///   - `lessThan`: Matches values that are less than the specified value.
///   - `lessThanOrEqual`: Matches values that are less than or equal to the specified value.
///   - `greaterThan`: Matches values that are greater than the specified value.
///   - `greaterThanOrEqual`: Matches values that are greater than or equal to the specified value.
///   - `between`: Matches values that fall inclusively between the specified lower and upper bounds.
///   - `in`: Matches values that are contained within the specified array of values.
///   - `exists`: Matches values that exist (non-nil).
///   - `notExists`: Matches values that do not exist (nil).
///
/// This enumeration conforms to `CustomStringConvertible` to provide a textual description of each operator.
public enum QueryOperator: CustomStringConvertible {

    case equal(AnyHashable)
    case notEqual(AnyHashable)
    case range(lower: AnyHashable, upper: AnyHashable)
    case contains(String)
    case startsWith(String)
    case endsWith(String)
    case lessThan(Any)
    case lessThanOrEqual(Any)
    case greaterThan(Any)
    case greaterThanOrEqual(Any)
    case between(lower: AnyHashable, upper: AnyHashable)
    case `in`([Any])
    case exists
    case notExists

    /// A textual representation of the query planner.
    ///
    /// This property provides a description of the query planner, which can be
    /// useful for debugging or logging purposes. It returns a `String` that
    /// represents the current state or details of the query planner.
    public var description: String {
        switch self {
        case .equal(let value):
            return "= \(value)"
        case .notEqual(let value):
            return "≠ \(value)"
        case .range(let lower, let upper):
            return "∈ [\(lower), \(upper)]"
        case .contains(let substring):
            return "contains '\(substring)'"
        case .startsWith(let prefix):
            return "starts with '\(prefix)'"
        case .endsWith(let suffix):
            return "ends with '\(suffix)'"
        case .lessThan(let value):
            return "< \(value)"
        case .lessThanOrEqual(let value):
            return "≤ \(value)"
        case .greaterThan(let value):
            return "> \(value)"
        case .greaterThanOrEqual(let value):
            return "≥ \(value)"
        case .between(let lower, let upper):
            return "between \(lower) and \(upper)"
        case .in(let values):
            return "in \(values)"
        case .exists:
            return "exists"
        case .notExists:
            return "not exists"
        }
    }
}


/// A structure responsible for planning and optimizing database queries.
///
/// `QueryPlanner` provides statistical information about indexes and shards
/// to assist in query execution planning.
///
/// - Properties:
///   - indexStats: A dictionary containing statistics for each index, where the key is the index name and the value is an `IndexStat` object.
///   - shardStats: An array of `ShardStat` objects representing statistics for each shard in the database.
public struct QueryPlanner {
    public let indexStats: [String: IndexStat]
    public let shardStats: [ShardStat]

    /// Initializes a new instance of the `QueryPlanner` class.
    ///
    /// - Parameters:
    ///   - indexStats: A dictionary containing statistics for each index, where the key is the index name and the value is an `IndexStat` object.
    ///   - shardStats: An array of `ShardStat` objects representing statistics for each shard.
    public init(indexStats: [String: IndexStat], shardStats: [ShardStat]) {
        self.indexStats = indexStats
        self.shardStats = shardStats
    }

    /// Optimizes the execution plan for a query based on the provided collection, predicates, and available indexes.
    ///
    /// - Parameters:
    ///   - collection: The name of the collection to query.
    ///   - predicates: An array of tuples representing the query predicates, where each tuple contains:
    ///     - `field`: The field name to apply the predicate on.
    ///     - `op`: The query operator to use for the predicate.
    ///   - availableIndexes: A list of available indexes for the collection.
    ///
    /// - Returns: An `ExecutionPlan` object representing the optimized query execution plan.
    public func optimize(
        collection: String,
        predicates: [(field: String, op: QueryOperator)],
        availableIndexes: [String]
    ) -> ExecutionPlan {
        var bestIndex: (field: String, cost: Int)? = nil
        var shardsToSkip = 0

        for index in availableIndexes {
            if let stat = indexStats[index] {
                for predicate in predicates where predicate.field == index {
                    let cost = calculateIndexCost(
                        predicate: predicate,
                        stat: stat
                    )
                    if bestIndex == nil || cost < bestIndex!.cost {
                        bestIndex = (field: index, cost: cost)
                    }
                }
            }
        }

        for shard in shardStats {
            if !shard.matchesAny(predicates: predicates) {
                shardsToSkip += 1
            }
        }

        let estimatedDocs =
            bestIndex?.cost ?? shardStats.reduce(0) { $0 + $1.docCount }
        let strategy: ExecutionPlan.IndexStrategy
        if let best = bestIndex, best.cost < estimatedDocs / 2 {
            strategy = best.cost < 100 ? .indexOnly : .hybrid
        } else {
            strategy = .fullScan
        }

        return ExecutionPlan(
            collection: collection,
            predicates: predicates,
            estimatedDocsToScan: estimatedDocs,
            shardsToSkip: shardsToSkip,
            usedIndex: bestIndex?.field,
            indexStrategy: strategy
        )
    }

    /// Calculates the cost of using an index for a given query predicate.
    ///
    /// - Parameters:
    ///   - predicate: A tuple containing the field name and the query operator
    ///     to be evaluated. The `field` represents the name of the field in the
    ///     database, and `op` represents the query operator (e.g., equality, range).
    ///   - stat: The statistical information about the index, which may include
    ///     details such as the number of entries or distribution of values.
    /// - Returns: An integer representing the calculated cost of using the index
    ///   for the given predicate. Lower values indicate a more efficient index usage.
    private func calculateIndexCost(
        predicate: (field: String, op: QueryOperator),
        stat: IndexStat
    ) -> Int {
        switch predicate.op {
        case .equal(let value):
            return stat.uniqueValuesCount / (stat.valueDistribution[value] ?? 1)
        case .range(let lower, let upper):
            return stat.estimateRange(lower: lower, upper: upper)
        default:
            return stat.totalCount
        }
    }
}
