import Foundation

public struct ShardMetadata: Codable {
    public var documentCount: Int
    public var createdAt: Date
    public var updatedAt: Date

    public init(
        documentCount: Int = 0,
        createdAt: Date = Date(),
        updatedAt: Date = Date()
    ) {
        self.documentCount = documentCount
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }
}

public class Shard {
    public let id: String
    public let url: URL
    public private(set) var metadata: ShardMetadata
    public let compressionMethod: CompressionMethod
    public let fileProtectionType: FileProtectionType

    public init(
        id: String,
        url: URL,
        metadata: ShardMetadata = .init(),
        compressionMethod: CompressionMethod = .none,
        fileProtectionType: FileProtectionType = .none
    ) {
        self.id = id
        self.url = url
        self.metadata = metadata
        self.compressionMethod = compressionMethod
        self.fileProtectionType = fileProtectionType
    }

    public func loadDocuments<T: Codable>() throws -> [T] {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return []
        }
        let compressedData = try Data(contentsOf: url)
        let data = try decompressData(compressedData, method: compressionMethod)
        return try JSONDecoder().decode([T].self, from: data)
    }

    public func saveDocuments<T: Codable>(_ documents: [T]) throws {
        let data = try JSONEncoder().encode(documents)
        let compressedData = try compressData(data, method: compressionMethod)
        try compressedData.write(to: url, options: .atomic)

        try FileManager.default.setAttributes(
            [.protectionKey: fileProtectionType],
            ofItemAtPath: url.path
        )

        metadata.documentCount = documents.count
        metadata.updatedAt = Date()
    }
}
