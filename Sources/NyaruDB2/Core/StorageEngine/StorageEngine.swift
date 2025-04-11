import Foundation

public actor StorageEngine {
    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    private let shardKey: String?  // se nil, não particiona; caso contrário, particiona pelo valor dessa chave
    // Cada coleção terá um gerenciador de shards (armazenado em um diretório próprio)
    private var activeShardManagers: [String: ShardManager] = [:]

    /// Inicializa o StorageEngine, informando o caminho base, o método de compactação e, opcionalmente, a shardKey.
    public init(
        path: String,
        shardKey: String? = nil,
        compressionMethod: CompressionMethod = .none
    ) {
        self.baseURL = URL(fileURLWithPath: path, isDirectory: true)
        self.shardKey = shardKey
        self.compressionMethod = compressionMethod
        try? FileManager.default.createDirectory(
            at: baseURL,
            withIntermediateDirectories: true
        )
    }

    // Insere um documento na coleção, usando particionamento se um shardKey tiver sido definido
    public func insertDocument<T: Codable>(_ document: T, collection: String)
        async throws
    {
        // Se o particionamento estiver ativo, extrai o valor do campo correspondente.
        var shardID: String
        if let key = shardKey {
            let jsonData = try JSONEncoder().encode(document)
            guard
                let dict = try JSONSerialization.jsonObject(
                    with: jsonData,
                    options: []
                ) as? [String: Any],
                let shardValue = dict[key] as? String
            else {
                throw NSError(
                    domain: "StorageEngine",
                    code: 1,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "Shard key '\(key)' não encontrado no documento"
                    ]
                )
            }
            shardID = shardValue
        } else {
            // Sem particionamento, usa um shard padrão para toda a coleção.
            shardID = "default"
        }

        // Obtém (ou cria) o ShardManager para a coleção
        var shardManager = activeShardManagers[collection]
        if shardManager == nil {
            let collectionURL = baseURL.appendingPathComponent(
                collection,
                isDirectory: true
            )
            try? FileManager.default.createDirectory(
                at: collectionURL,
                withIntermediateDirectories: true
            )
            shardManager = ShardManager(
                baseURL: collectionURL,
                compressionMethod: compressionMethod
            )
            activeShardManagers[collection] = shardManager
        }

        // Tenta obter o shard correspondente ao shardID ou o cria se não existir
        var shard: Shard
        do {
            shard = try shardManager!.getShard(byID: shardID)
        } catch {
            shard = try shardManager!.createShard(withID: shardID)
        }

        // Carrega os documentos existentes, insere o novo e salva o shard
        var documents: [T] = (try? shard.loadDocuments()) ?? []
        documents.append(document)
        try shard.saveDocuments(documents)
    }
}
