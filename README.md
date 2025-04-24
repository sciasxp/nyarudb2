# <h1 style="display: flex; align-items: center; gap: 0.5rem;"><img src="./img/nyaru.svg" alt="Logo" height="40" style="vertical-align: baseline;"/> <span>NyaruDB2</span> </h1>


**Lightweight, high-performance embedded database for Swift**  

[![SwiftPM](https://img.shields.io/badge/SwiftPM-compatible-brightgreen)](https://github.com/apple/swift-package-manager)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.md)

NyaruDB2 is an embedded database optimized for iOS and macOS applications, designed to handle large datasets efficiently using modern Swift Concurrency. It provides:

- **Automatic Sharding** with parallel I/O
- **Multi-Algorithm Compression** (GZIP, LZFSE, LZ4)
- **Actor-Safe B-Tree Indexing**
- **Cost-Based Query Planner** with shard pruning
- **Lazy Loading** via `AsyncThrowingStream`

---

## 🔖 Table of Contents
1. [Installation](#installation)
2. [Quick Start Example](#quick-start-example)
3. [Key Features](#key-features)
4. [Architecture](#architecture)
5. [Documentation](#documentation)
6. [Contributing](#contributing)
7. [License](#license)
8. [Acknowledgements](#acknowledgements)

---

## 📦 Installation

NyaruDB2 supports Swift Package Manager:

```swift
// swift-tools-version:5.9

let package = Package(
    name: "YourApp",
    dependencies: [
        .package(url: "https://github.com/galileostudio/NyaruDB2.git", from: "0.1.0-alpha")
    ],
    targets: [
        .target(
            name: "YourApp",
            dependencies: ["NyaruDB2"]
        )
    ]
)
```

**Requirements**:
- Xcode 15+
- Swift 5.9+
- iOS 15+ / macOS 12+

---

## 🚀 Quick Start Example

```swift
import NyaruDB2

// Define a model
struct User: Codable, Equatable {
    let id: Int
    let name: String
    let createdAt: String
}

// 1. Initialize the database
let db = try NyaruDB2(
    path: "NyaruDB_Demo",
    compressionMethod: .lzfse,
    fileProtectionType: .completeUntilFirstUserAuthentication
)

// 2. Create a partitioned collection
let users = try await db.createCollection(
    name: "Users",
    indexes: ["id"],
    partitionKey: "createdAt"
)

// 3. Bulk insert documents
try await users.bulkInsert([
    User(id: 1, name: "Alice", createdAt: "2024-01"),
    User(id: 2, name: "Bob", createdAt: "2024-02")
])

// 4. Perform a query
var query = try await users.query() as Query<User>
query.where(\User.id, .greaterThan(1))
let results = try await query.execute()
print(results)
```

---

## ✨ Key Features

### Performance
- **Sharded Storage**: automatic partitioning by configurable shard keys
- **Multi-Algorithm Compression**: GZIP, LZFSE, LZ4 via `Compression.framework`
- **Actor-Safe B-Tree Indexing**: configurable minimum degree for performance tuning

### Advanced Queries
- **Type-Safe Query Builder**: supports 15+ predicates (equal, range, contains, startsWith, etc.)
- **Lazy Loading**: `AsyncThrowingStream` for memory-efficient iterating
- **Cost-Based Query Planner**: selects optimal indexes and prunes shards using statistics

### Monitoring & Stats
- **StatsEngine**: `CollectionStats`, `GlobalStats` with shard count, document count, and sizes
- **IndexStats**: tracks value distribution, selectivity, access counts

---

## 🏗️ Architecture

```bash
NyaruDB2/
├── Sources/
│   ├── NyaruDB2/
│   │   ├── Core/
│   │   │   ├── Commons/          # FileProtection, DynamicDecoder
│   │   │   ├── IndexManager/     # BTreeIndex, IndexManager
│   │   │   ├── QueryEngine/      # Query, QueryPlanner, ExecutionPlan
│   │   │   ├── StatsEngine/      # CollectionStats, GlobalStats
│   │   │   └── StorageEngine/    # ShardManager, Compression, StorageEngine
│   │   ├── CollectionEngine/     # DocumentCollection, CollectionCatalog
│   │   └── NyaruDB2.swift        # public API
│   └── Benchmark/                # performance test suite
└── Tests/                        # unit and integration tests
```

---

## 📚 Documentation

Full API reference:  
🔗 https://nyarudb2.docs.example.com

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/awesome`
3. Commit your changes: `git commit -m "Add awesome feature"`
4. Push to your branch: `git push origin feature/awesome`
5. Open a Pull Request

Please see [CONTRIBUTING.md](.github/CONTRIBUTING.md) for details.

---

## 📜 License

Apache 2.0 © 2025 galileostudio. See [LICENSE](LICENSE.md).

---

## 🙏 Acknowledgements

Inspired by the original [NyaruDB](https://github.com/kelp404/NyaruDB) by `kelp404`. Many thanks for the foundational ideas.
