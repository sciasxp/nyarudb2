import Foundation

public actor StorageEngine {

    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    private let fileProtectionType: FileProtectionType
    private let shardKey: String?
    private var activeShardManagers = [String: ShardManager]()
    private var indexManagers = [String: IndexManager]()

    public enum StorageError: Error {
        case invalidDocument
        case partitionKeyNotFound(String)
        case indexKeyNotFound(String)
        case shardManagerCreationFailed
    }

    public init(
        path: String,
        shardKey: String? = nil,
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) throws {
        self.baseURL = URL(fileURLWithPath: path, isDirectory: true)
        self.shardKey = shardKey
        self.compressionMethod = compressionMethod
        self.fileProtectionType = fileProtectionType

        try FileManager.default.createDirectory(
            at: baseURL,
            withIntermediateDirectories: true
        )
    }

    public func insertDocument<T: Codable>(
        _ document: T,
        collection: String,
        indexField: String? = nil
    ) async throws {
        let jsonData = try JSONEncoder().encode(document)

        // 1. Otimização: Decodificação parcial apenas para chaves necessárias
        let partitionValue = try shardKey.flatMap { key in
            try extractValue(from: jsonData, key: key)
        }

        // 2. Gerenciamento eficiente de shards
        let shardManager = try await getOrCreateShardManager(for: collection)
        let shard = try await shardManager.getOrCreateShard(
            id: partitionValue ?? "default"
        )

        // 3. Operações de I/O assíncronas
        try await shard.appendDocument(document, jsonData: jsonData)

        // 4. Indexação otimizada
        if let indexField = indexField {
            let key = try extractValue(
                from: jsonData,
                key: indexField,
                forIndex: true
            )

            let indexManager = indexManagers[
                collection,
                default: IndexManager()
            ]
            indexManager.createIndex(for: indexField)
            indexManager.insert(index: indexField, key: key, data: jsonData)
            indexManagers[collection] = indexManager
        }
    }

    public func fetchDocuments<T: Codable>(from collection: String) async throws
        -> [T]
    {
        guard let shardManager = activeShardManagers[collection] else {
            return []
        }

        let shards = shardManager.allShards()

        // Cria um grupo de tarefas para carregar os documentos de cada shard de forma concorrente.
        return try await withThrowingTaskGroup(of: [T].self) { group in
            for shard in shards {
                group.addTask {
                    return try await shard.loadDocuments() as [T]
                }
            }

            // Combina os resultados de todas as tarefas
            var results: [T] = []
            for try await docs in group {
                results.append(contentsOf: docs)
            }
            return results
        }
    }

    public func deleteDocuments<T: Codable>(
        where predicate: (T) -> Bool,
        from collection: String
    ) async throws {
        // Obter o gerenciador de shards para a coleção.
        guard let shardManager = activeShardManagers[collection] else {
            return  // Se não existir, nada para deletar.
        }
        let shards = shardManager.allShards()

        // Para cada shard, carregar os documentos, filtrar os que NÃO satisfazem o predicado e gravar novamente.
        // Se a coleção estiver particionada, pode ser necessário iterar por todos os shards.
        for shard in shards {
            // Carrega os documentos do shard de forma assíncrona.
            let documents: [T] = try await shard.loadDocuments()

            // Filtra os documentos que não devem ser deletados.
            let newDocuments = documents.filter { !predicate($0) }

            // Se houve alteração, grava os novos documentos no shard.
            if newDocuments.count != documents.count {
                try await shard.saveDocuments(newDocuments)
            }
        }
    }

    public func updateDocument<T: Codable>(
        _ document: T,
        in collection: String,
        matching predicate: (T) -> Bool,
        indexField: String? = nil
    ) async throws {
        // 1. Codifica o documento atualizado
        let jsonData = try JSONEncoder().encode(document)

        // 2. Determina o shard em que o documento deve residir.
        // Se houver uma shardKey, extrai seu valor; caso contrário, usa "default".
        let shardManager = try await getOrCreateShardManager(for: collection)
        let shard: Shard
        if let key = shardKey {
            // Usa nossa função extractValue para obter o valor da chave de partição.
            let partitionValue = try extractValue(from: jsonData, key: key)
            shard = try await shardManager.getOrCreateShard(id: partitionValue)
        } else {
            shard = try await shardManager.getOrCreateShard(id: "default")
        }

        // 3. Carrega os documentos existentes no shard
        var documents: [T] = try await shard.loadDocuments()

        // 4. Procura pelo documento correspondente, usando o predicado fornecido.
        guard let indexToUpdate = documents.firstIndex(where: predicate) else {
            throw NSError(
                domain: "StorageEngine",
                code: 3,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "Documento não encontrado para update."
                ]
            )
        }

        // 5. Atualiza o documento na posição encontrada
        documents[indexToUpdate] = document

        // 6. Salva o array atualizado de documentos de volta ao shard
        try await shard.saveDocuments(documents)

        // 7. Se um campo de índice for informado, atualiza a entrada no índice.
        if let indexField = indexField {
            // Extraí o valor do campo de índice no documento atualizado.
            let key = try extractValue(
                from: jsonData,
                key: indexField,
                forIndex: true
            )

            // Obtém ou cria o IndexManager para a coleção
            let indexManager =
                indexManagers[collection]
                ?? {
                    let newManager = IndexManager()
                    newManager.createIndex(for: indexField)
                    indexManagers[collection] = newManager
                    return newManager
                }()

            // Atualiza a entrada no índice – a estratégia mais simples aqui é inserir a nova versão,
            // assumindo que se o índice já possui essa chave, o método de insert adicionará o novo dado.
            indexManager.insert(index: indexField, key: key, data: jsonData)
            indexManagers[collection] = indexManager
        }
    }

    public func bulkInsertDocuments<T: Codable>(
        _ documents: [T],
        collection: String,
        indexField: String? = nil
    ) async throws {
        // Se não houver documentos, simplesmente retorne
        guard !documents.isEmpty else { return }

        // Obtém ou cria o ShardManager para a coleção
        let shardManager = try await getOrCreateShardManager(for: collection)

        // Para atualizar os índices, vamos assegurar que o IndexManager exista para a coleção,
        // se indexField for especificado
        if let indexField = indexField, indexManagers[collection] == nil {
            let newManager = IndexManager()
            newManager.createIndex(for: indexField)
            indexManagers[collection] = newManager
        }

        // Agrupa os documentos (junto com seus jsonData) pelo valor da shardKey.
        // Se não houver shardKey, utiliza "default" como chave.
        var groups = [String: [(document: T, jsonData: Data)]]()
        for document in documents {
            let jsonData = try JSONEncoder().encode(document)

            let partitionValue =
                try shardKey.flatMap { key in
                    try extractValue(from: jsonData, key: key)
                } ?? "default"

            groups[partitionValue, default: []].append((document, jsonData))

            // Se há um campo de índice, atualiza o índice para cada documento.
            // Aqui usamos a função extractValue com forIndex: true para obter o valor do índice.
            if let indexField = indexField {
                let indexKey = try extractValue(
                    from: jsonData,
                    key: indexField,
                    forIndex: true
                )
                let indexManager = indexManagers[
                    collection,
                    default: {
                        let newManager = IndexManager()
                        newManager.createIndex(for: indexField)
                        indexManagers[collection] = newManager
                        return newManager
                    }()
                ]
                indexManager.insert(
                    index: indexField,
                    key: indexKey,
                    data: jsonData
                )
                indexManagers[collection] = indexManager
            }
        }

        // Para cada grupo (shard) carregue os documentos existentes, anexe os novos e salve uma única vez.
        for (shardId, groupDocuments) in groups {
            // Obtém ou cria o shard específico para a partição
            let shard = try await shardManager.getOrCreateShard(id: shardId)
            var existingDocs: [T] = try await shard.loadDocuments()

            // Adiciona todos os documentos do grupo
            for (doc, _) in groupDocuments {
                existingDocs.append(doc)
            }

            // Salva o conjunto atualizado de documentos no shard
            try await shard.saveDocuments(existingDocs)
        }
    }

    public func countDocuments(in collection: String) async throws -> Int {
        // Obtém o gerenciador de shards para a coleção
        guard let shardManager = activeShardManagers[collection] else {
            return 0
        }
        // Soma os documentCount de cada shard
        let shards = shardManager.allShards()
        var totalCount = 0
        for shard in shards {
            totalCount += shard.metadata.documentCount
        }
        return totalCount
    }

    public func dropCollection(_ collection: String) async throws {
        // Remove o diretório da coleção
        let collectionURL = baseURL.appendingPathComponent(
            collection,
            isDirectory: true
        )
        try FileManager.default.removeItem(at: collectionURL)

        // Remove a coleção dos gerenciadores ativos
        activeShardManagers.removeValue(forKey: collection)
        indexManagers.removeValue(forKey: collection)
    }

    public func listCollections() throws -> [String] {
        // Obtém todos os itens contidos no diretório base.
        let items = try FileManager.default.contentsOfDirectory(
            at: baseURL,
            includingPropertiesForKeys: nil
        )

        // Filtra apenas os itens que são diretórios (que são nossas coleções)
        let collections = items.filter { url in
            var isDirectory: ObjCBool = false
            FileManager.default.fileExists(
                atPath: url.path,
                isDirectory: &isDirectory
            )
            return isDirectory.boolValue
        }

        // Retorna os nomes dos diretórios
        return collections.map { $0.lastPathComponent }
    }
    
    public func getShardManagers(for collection: String) async throws -> [Shard] {
        guard let shardManager = activeShardManagers[collection] else { return [] }
        return shardManager.allShards()
    }

    // MARK: - Métodos auxiliares
    private func extractValue(
        from data: Data,
        key: String,
        forIndex: Bool = false
    ) throws -> String {
        // Converte os dados para um dicionário genérico
        guard
            let jsonObject = try JSONSerialization.jsonObject(
                with: data,
                options: []
            ) as? [String: Any]
        else {
            throw StorageError.invalidDocument
        }
        // Se não encontrar a chave, lança o erro apropriado
        guard let value = jsonObject[key] else {
            if forIndex {
                throw StorageError.indexKeyNotFound(key)
            }
            throw StorageError.partitionKeyNotFound(key)
        }
        // Converte o valor para String, independente do tipo original
        return String(describing: value)
    }

    private func getOrCreateShardManager(for collection: String) async throws
        -> ShardManager
    {
        if let existing = activeShardManagers[collection] {
            return existing
        }

        let collectionURL = baseURL.appendingPathComponent(
            collection,
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: collectionURL,
            withIntermediateDirectories: true
        )

        let newManager = ShardManager(
            baseURL: collectionURL,
            compressionMethod: compressionMethod
        )
        activeShardManagers[collection] = newManager
        return newManager
    }

    private func updateIndex(
        collection: String,
        indexField: String,
        jsonData: Data
    ) async throws {
        let indexManager =
            indexManagers[collection]
            ?? {
                let newManager = IndexManager()
                newManager.createIndex(for: indexField)
                indexManagers[collection] = newManager
                return newManager
            }()

        let key = try extractValue(
            from: jsonData,
            key: indexField,
            forIndex: true
        )
        indexManager.insert(index: indexField, key: key, data: jsonData)

    }
}

// MARK: - Extensões auxiliares
private struct DynamicCodingKey: CodingKey {
    var stringValue: String
    init?(stringValue: String) {
        self.stringValue = stringValue
    }

    var intValue: Int? { nil }
    init?(intValue: Int) { nil }
}
