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

        try saveMetadata(for: shard)

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
        shards.removeAll()
        guard
            let files = try? FileManager.default.contentsOfDirectory(
                at: baseURL,
                includingPropertiesForKeys: nil
            )
        else { return }
        for url in files where url.pathExtension == "shard" {
            let id = url.deletingPathExtension().lastPathComponent
            let metadata = (try? loadMetadata(from: url)) ?? ShardMetadata()
            shards[id] = Shard(id: id, url: url, metadata: metadata)
        }
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

        let candidateShards = shards.values
            .filter { $0.metadata.documentCount < threshold }
            .sorted { $0.metadata.createdAt < $1.metadata.createdAt }

        guard candidateShards.count > 1 else { return }

        let primaryShard = candidateShards.first!

        var accumulatedDocs: [String] = []

        for shard in candidateShards.dropFirst() {
            let docs: [String] = try await shard.loadDocuments()

            guard !docs.isEmpty else { continue }
            accumulatedDocs.append(contentsOf: docs)

            // Remove the shard from disk.
            do {
                try FileManager.default.removeItem(at: shard.url)
                let metaURL = shard.url.appendingPathExtension("meta.json")
                try? FileManager.default.removeItem(at: metaURL)
            } catch {
                print("Warning: Failed to remove shard \(shard.id): \(error)")
            }

            if let keyToRemove = shards.first(where: { $0.value === shard })?
                .key
            {
                shards.removeValue(forKey: keyToRemove)
            }
        }

        var primaryDocs: [String] = try await primaryShard.loadDocuments()

        primaryDocs.append(contentsOf: accumulatedDocs)

        try await primaryShard.saveDocuments(primaryDocs)

        primaryShard.updateMetadata(
            documentCount: primaryDocs.count,
            updatedAt: Date()
        )

        print(
            "Merged \(accumulatedDocs.count) documents from \(candidateShards.count - 1) small shards into shard \(primaryShard.id)"
        )
    }

    private func metadataURL(for shard: Shard) -> URL {
        shard.url.appendingPathExtension("meta.json")
    }

    private func saveMetadata(for shard: Shard) throws {
        let data = try JSONEncoder().encode(shard.metadata)
        do {
            try data.write(to: metadataURL(for: shard), options: .atomic)
        } catch {
            throw ShardManagerError.failedToSaveShard(shardID: shard.id)
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
}

public struct ShardMetadataInfo: Codable {
    public let id: String
    public let url: URL
    public let metadata: ShardMetadata
}
