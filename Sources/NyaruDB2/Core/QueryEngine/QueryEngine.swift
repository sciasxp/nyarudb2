import Foundation

// This is the refactored QueryEngine that now integrates with the QueryPlanner.
public struct Query<T: Codable> {
    private let collection: String
    private var predicates: [(field: String, op: QueryOperator)] = []
    private let storage: StorageEngine
    private let planner: QueryPlanner
    private var keyPathPredicates: [((T) -> AnyHashable, QueryOperator)] = []

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
    public mutating func `where`<V: Hashable>(
        _ keyPath: KeyPath<T, V>,
        _ op: QueryOperator
    ) {
        let predicate: (T) -> AnyHashable = { doc in
            return doc[keyPath: keyPath] as AnyHashable
        }
        keyPathPredicates.append((predicate, op))
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

    private func executeIndexOnly(plan: ExecutionPlan) async throws -> [T] {
        // Verifica se o plano usou um índice
        guard let indexField = plan.usedIndex else { return [] }

        // Filtra os predicados referentes ao campo de índice e extrai o valor de igualdade
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

    private func executeHybrid(plan: ExecutionPlan) async throws -> [T] {
        // Obtém todos os shards disponíveis para a coleção
        let allShards = try await storage.getShardManagers(for: collection)

        // Aplica o pruning dos shards com base em dados de ShardStat.
        // Por exemplo, se o shard não possuir documentos (docCount == 0), ele é ignorado.
        let shards = allShards.filter { shard in
            return shard.metadata.documentCount > 0
            // Para uma lógica mais avançada, por exemplo:
            // return shard.metadata.toShardStat().matches(any: predicates)
        }

        // Processa os shards em paralelo usando um TaskGroup para filtrar os documentos
        return try await withThrowingTaskGroup(of: [T].self) { group in
            for shard in shards {
                group.addTask {
                    let docs: [T] = try await shard.loadDocuments()
                    // Filtra os documentos usando a nova função evaluateDocument que utiliza KeyPathPredicates
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

    internal func evaluateDocument(_ doc: T) -> Bool {
        return keyPathPredicates.allSatisfy { (predicate, op) in
            evaluateValue(predicate(doc), op: op)
        }
    }

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

    private func isEqual(_ a: Any, _ b: AnyHashable) -> Bool {
        guard let aHashable = a as? AnyHashable else { return false }
        return aHashable == b
    }

    private func isInRange(_ value: Any, lower: AnyHashable, upper: AnyHashable)
        -> Bool
    {
        // Tenta comparar os valores convertendo para Double se possível.
        if let dValue = toDouble(value), let dLower = toDouble(lower),
            let dUpper = toDouble(upper)
        {
            return dValue >= dLower && dValue <= dUpper
        }

        // Se não for numérico, tenta comparar os valores convertendo para String.
        if let sValue = stringValue(value), let sLower = stringValue(lower),
            let sUpper = stringValue(upper)
        {
            return sValue >= sLower && sValue <= sUpper
        }

        return false
    }

    public func fetchStream(from storage: StorageEngine) -> AsyncThrowingStream<
        T, Error
    > {
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
