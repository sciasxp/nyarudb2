import Foundation

public protocol IndexKey: Hashable, Codable {}
extension String: IndexKey {}

public struct IndexMetrics {
    public var accessCount: Int = 0
    public var lastAccess: Date = .distantPast
    public var valueDistribution: [AnyHashable: Int] = [:]
}

public actor IndexManager<Key: IndexKey & Comparable> {
    private var indices: [String: BTreeIndex<Key>] = [:]
    private var createdIndexes: Set<String> = []
    private var metrics: [String: IndexMetrics] = [:]

    public init() {}

    /// Cria um índice para um campo específico.
    public func createIndex(for field: String, minimumDegree: Int = 2) async {
        if indices[field] == nil {
            let btree = BTreeIndex<Key>(minimumDegree: minimumDegree)
            indices[field] = btree
            createdIndexes.insert(field)
            metrics[field] = IndexMetrics()
        }
    }

    /// Insere um registro no índice para o campo informado.
    public func insert(index field: String, key: Key, data: Data) async {
        if var m = metrics[field] {
            m.accessCount += 1
            m.lastAccess = Date()
            m.valueDistribution[key, default: 0] += 1  // Increment the frequency for this key
            metrics[field] = m
        }

        guard let indexTree = indices[field] else {
            print(
                "Índice para o campo \(field) não foi criado. Utilize createIndex(for:) primeiro."
            )
            return
        }
        await indexTree.insert(key: key, data: data)
    }

    /// Pesquisa os dados no índice para o campo e valor fornecidos.
    public func search(_ field: String, value: Key) async -> [Data] {
        if var m = metrics[field] {
            m.accessCount += 1
            m.lastAccess = Date()
            metrics[field] = m
        }

        guard let indexTree = indices[field] else { return [] }
        return await indexTree.search(key: value) ?? []
    }

    public func getMetrics() -> [String: IndexMetrics] {
        return metrics
    }

    public func getIndexCounts() async -> [String: Int] {
        var counts = [String: Int]()
        for (field, btree) in indices {
            counts[field] = await btree.getTotalCount()
        }
        return counts
    }

    public func listIndexes() -> [String] {
        return Array(indices.keys)
    }

    public func dropIndex(for field: String) -> Bool {
        guard indices[field] != nil else {
            return false
        }
        indices.removeValue(forKey: field)
        createdIndexes.remove(field)
        metrics.removeValue(forKey: field)
        return true
    }

    public func upsertIndex(for field: String, jsonData: Data) async throws {
        await createIndex(for: field)

        let keyString = try DynamicDecoder.extractValue(
            from: jsonData,
            key: field,
            forIndex: true
        )

        guard let key = keyString as? Key else {
            print("Não foi possível converter a chave para o tipo Key.")
            return
        }

        await insert(index: field, key: key, data: jsonData)
    }

}
