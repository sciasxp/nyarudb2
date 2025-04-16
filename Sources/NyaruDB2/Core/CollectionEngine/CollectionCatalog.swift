import Foundation

/// Gerencia o registro e a recuperação de coleções configuradas no NyaruDB2.
/// Essa classe mantém internamente um dicionário onde a chave é o nome da coleção
/// e o valor é a instância de NyaruCollection com suas configurações específicas.
public class CollectionCatalog {

    /// Singleton para acesso global ao CollectionManager.
    public static let shared = CollectionCatalog()

    private var collections: [String: DocumentCollection] = [:]

    private init() {}

    /// Cria e registra uma nova coleção com as configurações fornecidas.
    ///
    /// - Parameters:
    ///   - db: A instância de NyaruDB2 que gerencia o armazenamento.
    ///   - name: Nome da coleção.
    ///   - indexes: Lista de campos que devem ser indexados.
    ///   - partitionKey: Campo usado para particionamento dos documentos na coleção.
    ///
    /// - Returns: A instância de NyaruCollection criada.
    public func createCollection(
        storage: StorageEngine,
        statsEngine: StatsEngine,
        name: String,
        indexes: [String] = [],
        partitionKey: String
    ) -> DocumentCollection {
        let newCollection = DocumentCollection(
            storage: storage,
            statsEngine: statsEngine,
            name: name,
            indexes: indexes,
            partitionKey: partitionKey
        )
        collections[name] = newCollection
        return newCollection
    }

    /// Retorna a coleção registrada com o nome especificado, se existir.
    public func getCollection(named name: String) -> DocumentCollection? {
        return collections[name]
    }

    public func removeCollection(named name: String, storage: StorageEngine)
        async throws
    {
        // Chama o drop no StorageEngine para remover os arquivos da coleção.
        try await storage.dropCollection(name)
        collections.removeValue(forKey: name)
    }

    public func updateCollectionConfiguration(
        named name: String,
        storage: StorageEngine,
        partitionKey: String
    ) async throws {
        // Atualiza a chave de partição da coleção.
        // Essa função pode ser expandida para atualizar também índices e demais configurações.
        // Aqui, supomos que o StorageEngine possui o método setPartitionKey.
        guard collections[name] != nil else {
            throw NSError(
                domain: "CollectionEngine",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Collection not found"]
            )
        }
        // Atualiza a configuração no StorageEngine.
        await storage.setPartitionKey(
            for: name,
            key: partitionKey
        )
    }

    /// Lista todas as coleções registradas.
    public func listCollections() -> [DocumentCollection] {
        return Array(collections.values)
    }
    
}
