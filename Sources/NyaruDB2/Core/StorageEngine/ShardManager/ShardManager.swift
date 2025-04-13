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

public struct ShardMetadataInfo {
    public let id: String
    public let url: URL
    public let metadata: ShardMetadata
}
