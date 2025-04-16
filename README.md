# NyaruDB2

**NyaruDB2** is a lightweight, high-performance embedded database for iOS apps, designed to handle large datasets efficiently with modern Swift concurrency features. It supports advanced data management capabilities including compression, sharding, indexing.

## Key Features

### Performance Optimizations
- **Sharded Storage Architecture**  
  Automatic partitioning using configurable shard keys (e.g., `"category"`) with parallel I/O operations
- **Multi-Algorithm Compression**  
  Supports GZIP, LZFSE, and LZ4 compression via Apple's Compression framework
- **B-Tree Indexing**  
  Concurrent-safe indexing system with configurable minimum degree

### Advanced Query Capabilities
- **Type-Safe Query Builder**  
  Supports 15+ predicate types including ranges, substring matching, and existence checks
- **Lazy Loading**  
  `AsyncThrowingStream` implementation for memory-efficient large dataset handling
- **Query Optimization**  
  Cost-based query planner with index selection and shard pruning

## Architecture Overview

```bash
NyaruDB2/
├── Sources/
│   ├── NyaruDB2/
│   │   ├── Core/
│   │   │   ├── Commons/          # FileProtection, DynamicDecoder
│   │   │   ├── IndexManager/     # B-Tree implementation (BTreeIndex.swift)
│   │   │   ├── QueryEngine/      # Query, QueryPlanner, ExecutionPlan
│   │   │   ├── StatsEngine/      # CollectionStats, GlobalStats
│   │   │   └── StorageEngine/    # ShardManager, Shard, Compression
│   │   ├── CollectionEngine/     # DocumentCollection, CollectionCatalog
│   │   └── NyaruDB2.swift        # Main public API
│   └── Benchmark/                # Performance test suite
└── Tests/
```

## Getting Started

### Requirements
- Swift 5.9+
- iOS 15+ / macOS 12+
- Xcode 15+

### Installation
Add to your `Package.swift`:
```swift
dependencies: [
    .package(url: "https://github.com/galileostudio/NyaruDB2.git", from: "1.0.0")
]
```

## Usage Example

```swift
import NyaruDB2

struct User: Codable {
    let id: Int
    let name: String
    let createdAt: String
}
```

- Initialize database
```swift
let db = try NyaruDB2(
    path: "NyaruDB_Demo",
    compressionMethod: .lzfse,
    fileProtectionType: .completeUntilFirstUserAuthentication
)
```

- Create collection with partition key
```swift
let users = try await db.createCollection(
    name: "Users",
    indexes: ["id"],
    partitionKey: "createdAt"
)
```
- Insert documents
```swift
try await users.bulkInsert([
    User(id: 1, name: "Alice", createdAt: "2024-01"),
    User(id: 2, name: "Bob", createdAt: "2024-02")
])
```

- Query with predicates
```swift
let query = try await users.query()
query.where(\User.id, .greaterThan(1))
let results = try await query.execute()
```

## Documentation

Explore full API reference at:  
[https://nyarudb2.docs.example.com](https://nyarudb2.docs.example.com)

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open Pull Request

## License

Apache 2.0 - See [LICENSE](LICENSE) file

---

**Contact**: [demetrius.albuquerque@yahoo.com.br](mailto:demetrius.albuquerque@yahoo.com.br)  


--- 
## Acknowledgements

NyaruDB2 was inspired by the original [NyaruDB](https://github.com/kelp404/NyaruDB) project created by [kelp404](https://github.com/kelp404). While NyaruDB2 has been completely rewritten with significant architectural changes (including sharding, compression, and modern Swift concurrency), we appreciate the foundational ideas from the initial implementation.

### Original Project Comparison
| Feature               | NyaruDB                           | NyaruDB2                          |
|-----------------------|-----------------------------------|-----------------------------------|
| **Architecture**      | Single-file storage               | Sharded design                    |
| **Concurrency**       | GCD-based async/sync              | Swift async/await (Actors)        |
| **Compression**       | None                              | GZIP, LZFSE, LZ4                  |
| **Indexing**          | Binary tree (Objective-C)         | Optimized B-Tree (Swift)          |
| **Query Optimization**| Basic filters                     | Cost-based query planner          |
| **Platform Support**  | iOS/macOS (Objective-C)           | iOS 15+/macOS 12+ (Swift)         |
| **License**           | MIT                               | Apache 2.0                        |