import Foundation

// MARK: - ExecutionPlan

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

    public enum IndexStrategy: CustomStringConvertible {
        case fullScan
        case indexOnly
        case hybrid

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
    case between(lower: Any, upper: Any)
    case `in`([Any])
    case exists
    case notExists

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

// MARK: - QueryPlanner

public struct QueryPlanner {
    public let indexStats: [String: IndexStat]
    public let shardStats: [ShardStat]

    public init(indexStats: [String: IndexStat], shardStats: [ShardStat]) {
        self.indexStats = indexStats
        self.shardStats = shardStats
    }

    public func optimize(
        collection: String,
        predicates: [(field: String, op: QueryOperator)],
        availableIndexes: [String]
    ) -> ExecutionPlan {
        var bestIndex: (field: String, cost: Int)? = nil
        var shardsToSkip = 0

        // Index selection
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

        // Shard pruning based on shard statistics
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
