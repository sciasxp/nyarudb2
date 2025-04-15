import Foundation


/// Gerencia o registro e a recuperação de coleções configuradas no NyaruDB2.
/// Essa classe mantém internamente um dicionário onde a chave é o nome da coleção
/// e o valor é a instância de NyaruCollection com suas configurações específicas.
public class CollectionManager {
    
    /// Singleton para acesso global ao CollectionManager.
    public static let shared = CollectionManager()
    
    private var collections: [String: NDBCollection] = [:]
    
    private init() { }
    
    /// Cria e registra uma nova coleção com as configurações fornecidas.
    ///
    /// - Parameters:
    ///   - db: A instância de NyaruDB2 que gerencia o armazenamento.
    ///   - name: Nome da coleção.
    ///   - indexes: Lista de campos que devem ser indexados.
    ///   - partitionKey: Campo usado para particionamento dos documentos na coleção.
    ///
    /// - Returns: A instância de NyaruCollection criada.
    public func createCollection(storage: StorageEngine, name: String, indexes: [String] = [], partitionKey: String) -> NDBCollection {
        let newCollection = NDBCollection(storage: storage, name: name, indexes: indexes, partitionKey: partitionKey)
        collections[name] = newCollection
        return newCollection
    }
    
    /// Retorna a coleção registrada com o nome especificado, se existir.
    public func getCollection(named name: String) -> NDBCollection? {
        return collections[name]
    }
    
    /// Lista todas as coleções registradas.
    public func listCollections() -> [NDBCollection] {
        return Array(collections.values)
    }
}
