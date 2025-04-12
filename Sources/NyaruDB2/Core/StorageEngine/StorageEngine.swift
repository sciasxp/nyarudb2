import Foundation

public actor StorageEngine {
    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    private let fileProtectionType: FileProtectionType
    private let shardKey: String?  // se nil, não particiona; caso contrário, particiona pelo valor dessa chave
    // Cada coleção terá um gerenciador de shards (armazenado em um diretório próprio)
    private var activeShardManagers: [String: ShardManager] = [:]
    private var indexManagers: [String: IndexManager] = [:] // Um IndexManager por coleção

    /// Inicializa o StorageEngine, informando o caminho base, o método de compactação e, opcionalmente, a shardKey.
    public init(
        path: String,
        shardKey: String? = nil,
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) {
        self.baseURL = URL(fileURLWithPath: path, isDirectory: true)
        self.shardKey = shardKey
        self.compressionMethod = compressionMethod
        self.fileProtectionType = fileProtectionType
        try? FileManager.default.createDirectory(
            at: baseURL,
            withIntermediateDirectories: true
        )
    }

    // Insere um documento na coleção, usando particionamento se um shardKey tiver sido definido
    public func insertDocument<T: Codable>(
        _ document: T,
        collection: String,
        indexField: String? = nil

    ) async throws {
        // Converte o documento para JSON para persistência e para extração dos valores.
        let jsonData = try JSONEncoder().encode(document)
        guard
            let dict = try JSONSerialization.jsonObject(
                with: jsonData,
                options: []
            ) as? [String: Any]
        else {
            throw NSError(
                domain: "StorageEngine",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Documento inválido"]
            )
        }

        // Define o shardID: se existir shardKey, usa seu valor; caso contrário, utiliza "default"
        var shardID = "default"
        if let key = shardKey {
            if let partitionVal = dict[key] as? String {
                shardID = partitionVal
            } else {
                throw NSError(
                    domain: "StorageEngine",
                    code: 1,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "Chave de particionamento '\(key)' não encontrada no documento"
                    ]
                )
            }
        }

        // Obtém ou cria o ShardManager para a coleção
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
                compressionMethod: .none,
                fileProtectionType: .none
            )
            activeShardManagers[collection] = shardManager
        }

        // Obtém ou cria o shard específico (partição) para o documento
        var shard: Shard
        do {
            shard = try shardManager!.getShard(byID: shardID)
        } catch {
            shard = try shardManager!.createShard(withID: shardID)
        }

        // Carrega os documentos existentes, acrescenta o novo e salva o shard
        var documents: [T] = (try? shard.loadDocuments()) ?? []
        documents.append(document)
        try shard.saveDocuments(documents)

        // Se um campo de índice foi informado, atualiza o índice no IndexManager
        if let indexField = indexField {
            var indexManager = indexManagers[collection]
            if indexManager == nil {
                indexManager = IndexManager()
                indexManager!.createIndex(for: indexField)
                indexManagers[collection] = indexManager
            }

            // Extraí o valor do campo a ser indexado e converte para String (ou use o wrapper que desejar)
            if let keyValue = dict[indexField] {
                // Aqui convertemos para String, de acordo com a atualização do IndexManager
                let keyString = String(describing: keyValue)
                indexManager?.insert(
                    index: indexField,
                    key: keyString,
                    data: jsonData
                )
            } else {
                throw NSError(
                    domain: "StorageEngine",
                    code: 2,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "Campo de índice '\(indexField)' não encontrado no documento"
                    ]
                )
            }
        }
    }
}
