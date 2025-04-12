import Foundation

public class IndexManager {
    // Agora usamos String, que conforma Comparable, para as chaves do índice.
    private var indices: [String: BTreeIndex<String>] = [:]
    
    public init() {}
    
    /// Cria um índice para um campo específico.
    public func createIndex(for field: String, minimumDegree: Int = 2) {
        indices[field] = BTreeIndex<String>(minimumDegree: minimumDegree)
    }
    
    /// Insere um registro no índice relativo ao campo.
    /// Converte a chave para String usando String(describing:).
    public func insert(index field: String, key: AnyHashable, data: Data) {
        guard let indexTree = indices[field] else {
            print("Índice para o campo \(field) não foi criado. Utilize createIndex(for:) primeiro.")
            return
        }
        let strKey = String(describing: key)
        indexTree.insert(key: strKey, data: data)
    }
    
    /// Pesquisa os dados no índice para um dado campo e valor.
    /// Converte o valor para String para a busca.
    public func search(_ field: String, value: AnyHashable) -> [Data] {
        let strValue = String(describing: value)
        return indices[field]?.search(key: strValue) ?? []
    }
}
