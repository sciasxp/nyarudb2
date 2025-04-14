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

    public init(
        collectionName: String,
        numberOfShards: Int,
        totalDocuments: Int,
        totalSizeInBytes: UInt64,
        shardDetails: [ShardMetadataInfo]
    ) {
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

public struct IndexStat {
    public let totalCount: Int
    public let uniqueValuesCount: Int
    public let valueDistribution: [AnyHashable: Int]

    public func estimateRange(lower: AnyHashable, upper: AnyHashable) -> Int {
        // Simplified range cost: adjust as needed.
        return totalCount / 4
    }
}

public struct ShardStat {
    public let docCount: Int
    public let fieldRanges: [String: (min: AnyHashable, max: AnyHashable)]

    public func matchesAny(predicates: [(field: String, op: QueryOperator)])
        -> Bool
    {
        for predicate in predicates {
            if let range = fieldRanges[predicate.field] {
                switch predicate.op {
                case .equal(let value):
                    if let valueInt = value as? Int,
                        let minInt = range.min as? Int,
                        let maxInt = range.max as? Int
                    {
                        if valueInt >= minInt && valueInt <= maxInt {
                            return true
                        }
                    } else if let valueDouble = value as? Double,
                        let minDouble = range.min as? Double,
                        let maxDouble = range.max as? Double
                    {
                        if valueDouble >= minDouble && valueDouble <= maxDouble
                        {
                            return true
                        }
                    } else if let valueString = value as? String,
                        let minString = range.min as? String,
                        let maxString = range.max as? String
                    {
                        if valueString >= minString && valueString <= maxString
                        {
                            return true
                        }
                    }
                case .range(let lower, let upper):
                    if let lowerInt = lower as? Int,
                        let upperInt = upper as? Int,
                        let minInt = range.min as? Int,
                        let maxInt = range.max as? Int
                    {
                        if upperInt >= minInt && lowerInt <= maxInt {
                            return true
                        }
                    } else if let lowerDouble = lower as? Double,
                        let upperDouble = upper as? Double,
                        let minDouble = range.min as? Double,
                        let maxDouble = range.max as? Double
                    {
                        if upperDouble >= minDouble && lowerDouble <= maxDouble
                        {
                            return true
                        }
                    } else if let lowerString = lower as? String,
                        let upperString = upper as? String,
                        let minString = range.min as? String,
                        let maxString = range.max as? String
                    {
                        if upperString >= minString && lowerString <= maxString
                        {
                            return true
                        }
                    }
                default:
                    break
                }
            }
        }
        return false
    }
}

public actor StatsEngine {

    private let storage: StorageEngine

    public init(storage: StorageEngine) {
        self.storage = storage
    }

    // Obtém estatísticas para uma coleção específica
    public func getCollectionStats(_ collection: String) async throws
        -> CollectionStats
    {
        // Aproveita a função fetchDocuments e countDocuments já implementadas no StorageEngine
        let shards = try await storage.getShardManagers(for: collection)  // Supondo que StorageEngine exponha os ShardManagers para essa coleção ou um método auxiliar similar.

        var totalDocs = 0
        var totalSize: UInt64 = 0
        var details: [ShardMetadataInfo] = []

        for shard in shards {
            totalDocs += shard.metadata.documentCount
            // Para obter o tamanho em bytes do arquivo do shard, usamos FileManager
            if let attributes = try? FileManager.default.attributesOfItem(
                atPath: shard.url.path
            ),
                let fileSize = attributes[.size] as? UInt64
            {
                totalSize += fileSize
            }
            details.append(
                ShardMetadataInfo(
                    id: shard.id,
                    url: shard.url,
                    metadata: shard.metadata
                )
            )
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

    public func getIndexStats() async -> [String: IndexStat] {
        var stats = [String: IndexStat]()
        // This example assumes that StorageEngine manages indexManagers in a dictionary keyed by collection.
        // Replace this dummy data with actual values from your index managers.
        for (collection, _) in await storage.indexManagers {
            let dummyStat = IndexStat(
                totalCount: 100,  // e.g., total number of entries
                uniqueValuesCount: 10,  // e.g., count of unique keys
                valueDistribution: [:]  // e.g., frequency histogram (empty for now)
            )
            stats[collection] = dummyStat
        }
        return stats
    }

    public func getShardStats() async throws -> [ShardStat] {
        var shardStats = [ShardStat]()
        for (_, manager) in await storage.activeShardManagers {
            for shard in manager.allShards() {
                let stat = ShardStat(
                    docCount: shard.metadata.documentCount,
                    fieldRanges: [:]  // Placeholder; replace with actual range data if available.
                )
                shardStats.append(stat)
            }
        }
        return shardStats
    }
}
