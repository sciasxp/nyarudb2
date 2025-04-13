//
//  StatsEngine.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 13/04/25.
//
import Foundation

public struct CollectionStats: Codable {
    public let collectionName: String
    public let numberOfShards: Int
    public let totalDocuments: Int
    public let totalSizeInBytes: UInt64
    public let shardDetails: [ShardMetadataInfo]
    
    public init(collectionName: String, numberOfShards: Int, totalDocuments: Int, totalSizeInBytes: UInt64, shardDetails: [ShardMetadataInfo]) {
            self.collectionName = collectionName
            self.numberOfShards = numberOfShards
            self.totalDocuments = totalDocuments
            self.totalSizeInBytes = totalSizeInBytes
            self.shardDetails = shardDetails
        }
}

public struct GlobalStats: Codable {
    public let totalCollections: Int
    public let totalDocuments: Int
    public let totalSizeInBytes: UInt64
}

public actor StatsEngine {

    private let storage: StorageEngine

    public init(storage: StorageEngine) {
        self.storage = storage
    }
    
    // Obtém estatísticas para uma coleção específica
    public func getCollectionStats(_ collection: String) async throws -> CollectionStats {
        // Aproveita a função fetchDocuments e countDocuments já implementadas no StorageEngine
        let shards = try await storage.getShardManagers(for: collection)  // Supondo que StorageEngine exponha os ShardManagers para essa coleção ou um método auxiliar similar.
        
        var totalDocs = 0
        var totalSize: UInt64 = 0
        var details: [ShardMetadataInfo] = []
        
        for shard in shards {
            totalDocs += shard.metadata.documentCount
            // Para obter o tamanho em bytes do arquivo do shard, usamos FileManager
            if let attributes = try? FileManager.default.attributesOfItem(atPath: shard.url.path),
               let fileSize = attributes[.size] as? UInt64 {
                totalSize += fileSize
            }
            details.append(ShardMetadataInfo(id: shard.id, url: shard.url, metadata: shard.metadata))
        }
        
        return CollectionStats(
            collectionName: collection,
            numberOfShards: shards.count,
            totalDocuments: totalDocs,
            totalSizeInBytes: totalSize,
            shardDetails: details
        )
    }
    
    // Obtém estatísticas globais do banco (todas as coleções)
    public func getGlobalStats() async throws -> GlobalStats {
        // Suponha que o StorageEngine tenha um método para listar coleções, ex: listCollections()
        let collections = try await storage.listCollections()
        var globalDocuments = 0
        var globalSize: UInt64 = 0
        
        for collection in collections {
            let stats = try await getCollectionStats(collection)
            globalDocuments += stats.totalDocuments
            globalSize += stats.totalSizeInBytes
        }
        
        return GlobalStats(
            totalCollections: collections.count,
            totalDocuments: globalDocuments,
            totalSizeInBytes: globalSize
        )
    }
}
