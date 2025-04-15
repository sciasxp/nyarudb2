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
    private var documentCache: NSCache<NSString, NSArray> = NSCache()

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

    public func updateMetadata(documentCount: Int, updatedAt: Date) {
        self.metadata.documentCount = documentCount
        self.metadata.updatedAt = updatedAt
    }

    public func loadDocuments<T: Codable>() async throws -> [T] {
        let key = cacheKey(for: T.self)
        // Tenta obter os documentos do cache
        if let cached = documentCache.object(forKey: key) as? [T] {
            return cached
        }

        // Se o arquivo não existir, retorna array vazio
        guard FileManager.default.fileExists(atPath: url.path) else {
            return []
        }

        let compressedData = try Data(contentsOf: url)
        let data = try decompressData(compressedData, method: compressionMethod)
        let docs = try JSONDecoder().decode([T].self, from: data)
        // Armazena os documentos no cache
        documentCache.setObject(docs as NSArray, forKey: key)
        return docs
    }

    public func saveDocuments<T: Codable>(_ documents: [T]) async throws {
        let data = try JSONEncoder().encode(documents)
        let compressedData = try compressData(data, method: compressionMethod)
        try compressedData.write(to: url, options: .atomic)

        try FileManager.default.setAttributes(
            [.protectionKey: fileProtectionType],
            ofItemAtPath: url.path
        )

        metadata.documentCount = documents.count
        metadata.updatedAt = Date()

        let key = cacheKey(for: T.self)
        documentCache.setObject(documents as NSArray, forKey: key)
    }

    public func appendDocument<T: Codable>(_ document: T, jsonData: Data)
        async throws
    {
        var documents: [T] = (try? await loadDocuments()) ?? []
        documents.append(document)
        try await saveDocuments(documents)
    }

    public func loadDocumentsLazy<T: Codable>() -> AsyncThrowingStream<T, Error>
    {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    let docs: [T] = try await self.loadDocuments()
                    for doc in docs {
                        continuation.yield(doc)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    private func cacheKey<T: Codable>(for type: T.Type) -> NSString {
        return "\(String(describing: type))_docs" as NSString
    }

}
