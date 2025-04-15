import Foundation

public enum ShardManagerError: Error {
    case shardNotFound(shardID: String)
    case shardAlreadyExists(shardID: String)
    case failedToSaveShard(shardID: String)
    case unknown(Error)
}

public class ShardManager {
    private var shards: [String: Shard] = [:]
    private let baseURL: URL
    private let compressionMethod: CompressionMethod
    public let fileProtectionType: FileProtectionType

    private var autoMergeTask: Task<Void, Never>?

    public init(
        baseURL: URL,
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) {
        self.baseURL = baseURL

        try? FileManager.default.createDirectory(
            at: baseURL,
            withIntermediateDirectories: true
        )
        self.compressionMethod = compressionMethod
        self.fileProtectionType = fileProtectionType

        startAutoMergeProcess()
    }

    deinit {
        autoMergeTask?.cancel()
    }

    public func createShard(withID id: String) async throws -> Shard {
        guard shards[id] == nil else {
            throw ShardManagerError.shardAlreadyExists(shardID: id)
        }

        let shardURL = baseURL.appendingPathComponent("\(id).nyaru")

        let shard = Shard(
            id: id,
            url: shardURL,
            compressionMethod: compressionMethod,
            fileProtectionType: fileProtectionType
        )

        shards[id] = shard

        try await shard.saveDocuments([] as [String])

        return shard
    }

    public func getOrCreateShard(id: String) async throws -> Shard {
        if let shard = shards[id] {
            return shard
        }
        return try await createShard(withID: id)
    }

    public func getShard(byID id: String) throws -> Shard {
        guard let shard = shards[id] else {
            throw ShardManagerError.shardNotFound(shardID: id)
        }
        return shard
    }

    public func allShardInfo() -> [ShardMetadataInfo] {
        shards.values.map { shard in
            ShardMetadataInfo(
                id: shard.id,
                url: shard.url,
                metadata: shard.metadata
            )
        }
    }

    public func loadShards() {
        guard
            let files = try? FileManager.default.contentsOfDirectory(
                at: baseURL,
                includingPropertiesForKeys: nil
            )
        else {
            print(
                "[ShardManager] Failed to read directory contents at \(baseURL)"
            )
            return
        }

        let loadedShards =
            files
            .filter { $0.pathExtension == "shard" }
            .compactMap { url -> (String, Shard)? in
                let id = url.deletingPathExtension().lastPathComponent
                do {
                    let metadata = try loadMetadata(from: url)
                    print("[ShardManager] Successfully loaded shard \(id)")
                    return (id, Shard(id: id, url: url, metadata: metadata))
                } catch {
                    print(
                        "[ShardManager] Error loading metadata for shard \(id): \(error.localizedDescription)"
                    )
                    print(
                        "[ShardManager] Creating shard \(id) with default metadata"
                    )
                    return (
                        id, Shard(id: id, url: url, metadata: ShardMetadata())
                    )
                }
            }

        shards = Dictionary(uniqueKeysWithValues: loadedShards)
        print("[ShardManager] Loaded \(shards.count) shards")
    }

    private func startAutoMergeProcess() {
        autoMergeTask = Task<Void, Never> { () async -> Void in
            while !Task.isCancelled {
                do {
                    // Check and merge small shards (this is a placeholder for the actual merge logic)
                    try await mergeSmallShards()
                } catch {
                    print("Auto-merge error: \(error)")
                }
                // Sleep for 60 seconds before checking again.
                try? await Task.sleep(nanoseconds: 60_000_000_000)
            }
        }
    }

    private func mergeSmallShards() async throws {
        let threshold = 100

        // Seleção de shards candidatos
        let candidateShards = shards.values
            .filter { $0.metadata.documentCount < threshold }
            .sorted { $0.metadata.createdAt < $1.metadata.createdAt }

        guard candidateShards.count > 1 else { return }

        let primaryShard = candidateShards.first!

        // Processamento dos shards secundários
        let accumulatedDocs = try await candidateShards.dropFirst()
            .asyncCompactMap { try await processAndRemoveShard($0) }
            .flatMap { $0 }

        // Processamento do shard principal
        let primaryDocs = try await loadShardDocuments(primaryShard)
        let mergedDocs = primaryDocs + accumulatedDocs

        // Salvando os documentos mesclados
        try saveMergedDocuments(mergedDocs, to: primaryShard)

        // Atualização de metadados
        primaryShard.updateMetadata(
            documentCount: mergedDocs.count,
            updatedAt: Date()
        )

        print(
            "Merged \(accumulatedDocs.count) documents from \(candidateShards.count - 1) small shards into shard \(primaryShard.id)"
        )
    }

    // MARK: - Funções auxiliares

    private func processAndRemoveShard(_ shard: Shard) async throws -> [Any]? {
        do {
            let docs = try await loadShardDocuments(shard)
            try removeShardFiles(shard)
            removeShardFromMemory(shard)
            return docs.isEmpty ? nil : docs
        } catch {
            print("Warning: Failed to process shard \(shard.id): \(error)")
            return nil
        }
    }

    private func loadShardDocuments(_ shard: Shard) async throws -> [Any] {
        let data = try Data(contentsOf: shard.url)
        let decompressed = try decompressData(
            data,
            method: shard.compressionMethod
        )
        let json = try JSONSerialization.jsonObject(
            with: decompressed,
            options: []
        )
        return (json as? [Any]) ?? []
    }

    private func saveMergedDocuments(_ documents: [Any], to shard: Shard) throws
    {
        let jsonData = try JSONSerialization.data(
            withJSONObject: documents,
            options: []
        )
        let compressedData = try compressData(
            jsonData,
            method: shard.compressionMethod
        )
        try compressedData.write(to: shard.url, options: .atomic)
    }

    private func removeShardFiles(_ shard: Shard) throws {
        try FileManager.default.removeItem(at: shard.url)
        let metaURL = shard.url.appendingPathExtension("meta.json")
        try? FileManager.default.removeItem(at: metaURL)
    }

    private func removeShardFromMemory(_ shard: Shard) {
        if let key = shards.first(where: { $0.value === shard })?.key {
            shards.removeValue(forKey: key)
        }
    }

    private func loadMetadata(from shardURL: URL) throws -> ShardMetadata {
        let metaURL = shardURL.appendingPathExtension("meta.json")
        let data = try Data(contentsOf: metaURL)
        return try JSONDecoder().decode(ShardMetadata.self, from: data)
    }

    public func allShards() -> [Shard] {
        return Array(shards.values)
    }

    public func cleanupEmptyShards() async throws {
        try shards
            .filter { $0.value.metadata.documentCount == 0 }
            .forEach { key, shard in
                try FileManager.default.removeItem(at: shard.url)
                try? FileManager.default.removeItem(
                    at: shard.url.appendingPathExtension("meta.json")
                )
                shards.removeValue(forKey: key)
            }
    }

}

extension Sequence {
    func asyncCompactMap<T>(
        _ transform: (Element) async throws -> T?
    ) async rethrows -> [T] {
        var results = [T]()
        for element in self {
            if let transformed = try await transform(element) {
                results.append(transformed)
            }
        }
        return results
    }
}
