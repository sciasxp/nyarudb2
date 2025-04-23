//
//  StatsEngine.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 13/04/25.
//
import Foundation

/// A structure representing metadata information for a shard.
///
/// `ShardMetadataInfo` contains details about a specific shard, including its unique identifier,
/// its location as a URL, and its associated metadata.
///
/// - Properties:
///   - id: A unique identifier for the shard.
///   - url: The URL location of the shard.
///   - metadata: The metadata associated with the shard.
public struct ShardMetadataInfo: Codable {
    public let id: String
    public let url: URL
    public let metadata: ShardMetadata
}

/// A structure representing statistics for a collection in the database.
///
/// `CollectionStats` provides detailed information about a specific collection,
/// including its name, the number of shards, the total number of documents,
/// the total size in bytes, and metadata about each shard.
///
/// - Properties:
///   - collectionName: The name of the collection.
///   - numberOfShards: The number of shards associated with the collection.
///   - totalDocuments: The total number of documents stored in the collection.
///   - totalSizeInBytes: The total size of the collection in bytes.
///   - shardDetails: An array of metadata information for each shard.
public struct CollectionStats: Codable {
    public let collectionName: String
    public let numberOfShards: Int
    public let totalDocuments: Int
    public let totalSizeInBytes: UInt64
    public let shardDetails: [ShardMetadataInfo]

    /// Initializes a new instance of `StatsEngine` with the specified parameters.
    ///
    /// - Parameters:
    ///   - collectionName: The name of the collection being analyzed.
    ///   - numberOfShards: The total number of shards in the collection.
    ///   - totalDocuments: The total number of documents in the collection.
    ///   - totalSizeInBytes: The total size of the collection in bytes.
    ///   - shardDetails: An array containing metadata information for each shard.
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

/// A structure representing global statistics for the database.
///
/// `GlobalStats` provides an overview of the database's state, including
/// the total number of collections, documents, and the total size in bytes.
///
/// - Properties:
///   - totalCollections: The total number of collections in the database.
///   - totalDocuments: The total number of documents across all collections.
///   - totalSizeInBytes: The total size of the database in bytes.
public struct GlobalStats: Codable {
    public let totalCollections: Int
    public let totalDocuments: Int
    public let totalSizeInBytes: UInt64
}

/// A structure representing statistical information about an index.
///
/// This structure provides details about the total number of entries, 
/// the count of unique values, the distribution of values, 
/// and access-related metadata for an index.
///
/// - Properties:
///   - totalCount: The total number of entries in the index.
///   - uniqueValuesCount: The number of unique values in the index.
///   - valueDistribution: A dictionary mapping each unique value to its occurrence count.
///   - accessCount: The number of times the index has been accessed.
///   - lastAccess: The date and time when the index was last accessed.
public struct IndexStat {
    public var totalCount: Int
    public var uniqueValuesCount: Int
    public var valueDistribution: [AnyHashable: Int]
    public var accessCount: Int
    public var lastAccess: Date

    /// The selectivity of the data, represented as a `Double`.
    ///
    /// Selectivity is a measure of how specific or filtered the data is,
    /// typically used in database query optimization to determine the
    /// efficiency of an index or query. A lower value indicates higher
    /// selectivity, meaning fewer rows match the criteria.
    public var selectivity: Double {
        guard totalCount > 0 else { return 1.0 }
        return Double(uniqueValuesCount) / Double(totalCount)
    }

    /// Estimates the number of elements within the specified range.
    ///
    /// - Parameters:
    ///   - lower: The lower bound of the range as an `AnyHashable` value.
    ///   - upper: The upper bound of the range as an `AnyHashable` value.
    /// - Returns: An integer representing the estimated count of elements within the range.
    public func estimateRange(lower: AnyHashable, upper: AnyHashable) -> Int {
        /// Converts a given `AnyHashable` value to a `Double` if possible.
        ///
        /// - Parameter value: The `AnyHashable` value to be converted.
        /// - Returns: A `Double` representation of the value if the conversion is successful, otherwise `nil`.
        func toDouble(_ value: AnyHashable) -> Double? {
            if let number = value as? NSNumber {
                return number.doubleValue
            }
            if let string = value as? String, let d = Double(string) {
                return d
            }
            return nil
        }

        var cost = 0
        for (key, frequency) in valueDistribution {
            if let keyValue = toDouble(key),
                let lowerValue = toDouble(lower),
                let upperValue = toDouble(upper),
                keyValue >= lowerValue, keyValue <= upperValue
            {
                cost += frequency
            }
        }

        return cost > 0 ? cost : totalCount / 4
    }
}

/// Represents statistics for a shard in the database.
/// 
/// - Properties:
///   - docCount: The total number of documents in the shard.
///   - fieldRanges: A dictionary mapping field names to their respective minimum and maximum values.
///                  The values are represented as `AnyHashable` to allow for flexibility in data types.
public struct ShardStat {
    public let docCount: Int
    public let fieldRanges: [String: (min: AnyHashable, max: AnyHashable)]

    /// Evaluates whether the current instance matches any of the provided predicates.
    ///
    /// - Parameter predicates: An array of tuples where each tuple contains:
    ///   - `field`: The name of the field to evaluate.
    ///   - `op`: The query operator to apply for the evaluation.
    /// - Returns: A Boolean value indicating whether any of the predicates match.
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

/// An actor responsible for managing and processing statistical data within the database.
/// The `StatsEngine` interacts with the underlying storage engine to perform its operations.
public actor StatsEngine {

    private let storage: StorageEngine

    /// Initializes a new instance of the `StatsEngine` class.
    ///
    /// - Parameter storage: An instance of `StorageEngine` used to manage the underlying storage for the stats engine.
    public init(storage: StorageEngine) {
        self.storage = storage
    }

    
    /// Retrieves statistical information about a specified collection.
    ///
    /// - Parameter collection: The name of the collection for which to retrieve statistics.
    /// - Returns: A `CollectionStats` object containing the statistics of the specified collection.
    /// - Throws: An error if the operation fails.
    /// - Note: This is an asynchronous function and must be awaited.
    public func getCollectionStats(_ collection: String) async throws
        -> CollectionStats
    {
    
        let shards = try await storage.getShardManagers(for: collection)

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


    /// Retrieves the global statistics for the database.
    ///
    /// This asynchronous function fetches and returns an instance of `GlobalStats`,
    /// which contains aggregated statistical data about the database.
    ///
    /// - Returns: A `GlobalStats` object containing the global statistics.
    /// - Throws: An error if the operation fails during the retrieval process.
    public func getGlobalStats() async throws -> GlobalStats {
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

    /// Asynchronously retrieves statistics for all indexes.
    ///
    /// - Returns: A dictionary where the keys are index names (`String`) and the values are `IndexStat` objects containing the statistics for each index.
    public func getIndexStats() async -> [String: IndexStat] {
        var stats = [String: IndexStat]()
        for (collection, indexManager) in await storage.indexManagers {
            let metrics = await indexManager.getMetrics()
            let counts = await indexManager.getIndexCounts()
            var combinedTotalCount = 0
            var combinedUniqueValuesCount = 0
            var combinedDistribution = [AnyHashable: Int]()
            var combinedAccessCount = 0
            var latestAccess = Date.distantPast

            // Combine metrics from each index field of the collection.
            for (field, metric) in metrics {
                let fieldTotal = counts[field] ?? 0
                // In a proper implementation, the B-Tree should yield the count of unique keys.
                // Here, we assume fieldTotal as the unique count for simplicity.
                let fieldUnique = fieldTotal

                combinedTotalCount += fieldTotal
                combinedUniqueValuesCount += fieldUnique
                combinedAccessCount += metric.accessCount
                if metric.lastAccess > latestAccess {
                    latestAccess = metric.lastAccess
                }

                // Aggregate the value distribution by summing the counts for each key.
                for (key, count) in metric.valueDistribution {
                    combinedDistribution[key, default: 0] += count
                }
            }

            let indexStat = IndexStat(
                totalCount: combinedTotalCount,
                uniqueValuesCount: combinedUniqueValuesCount,
                valueDistribution: combinedDistribution,
                accessCount: combinedAccessCount,
                lastAccess: latestAccess
            )
            stats[collection] = indexStat
        }
        return stats
    }

    /// Retrieves statistics for all shards.
    ///
    /// This asynchronous function fetches and returns an array of `ShardStat` objects,
    /// which contain statistical information about each shard in the database.
    ///
    /// - Returns: An array of `ShardStat` objects representing the statistics of each shard.
    /// - Throws: An error if the operation fails during the retrieval process.
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

    /// Updates the metadata for a specified collection.
    ///
    /// This asynchronous function performs an update operation on the metadata
    /// associated with the given collection. It may throw an error if the update
    /// process encounters any issues.
    ///
    /// - Parameter collection: The name of the collection whose metadata needs to be updated.
    /// - Throws: An error if the metadata update fails.
    /// - Returns: This function does not return a value.
    public func updateCollectionMetadata(for collection: String) async throws {
        let stats = try await getCollectionStats(collection)

        // Para obter o diretório da coleção, podemos chamar um método público em StorageEngine.
        // Se não existir, crie um método, por exemplo, 'collectionDirectory(for:)' em StorageEngine.
        let collectionDirectory = try await storage.collectionDirectory(
            for: collection
        )

        let aggregatedMetaURL = collectionDirectory.appendingPathComponent(
            "\(collection).nyaru.meta.json"
        )
        let metadataData = try JSONEncoder().encode(stats)
        try metadataData.write(to: aggregatedMetaURL, options: .atomic)
    }
}
