import Foundation

public protocol IndexKey: Hashable, Codable {}
extension String: IndexKey {}

public actor IndexManager<Key: IndexKey & Comparable> {
    private var indices: [String: BTreeIndex<Key>] = [:]
    
    public init() {}
    
    /// Cria um índice para um campo específico.
    public func createIndex(for field: String, minimumDegree: Int = 2) async {
        let btree = BTreeIndex<Key>(minimumDegree: minimumDegree)
        indices[field] = btree
    }
    
    /// Insere um registro no índice para o campo informado.
    public func insert(index field: String, key: Key, data: Data) async {
        guard let indexTree = indices[field] else {
            print("Índice para o campo \(field) não foi criado. Utilize createIndex(for:) primeiro.")
            return
        }
        await indexTree.insert(key: key, data: data)
    }
    
    /// Pesquisa os dados no índice para o campo e valor fornecidos.
    public func search(_ field: String, value: Key) async -> [Data] {
        guard let indexTree = indices[field] else { return [] }
        return await indexTree.search(key: value) ?? []
    }
}