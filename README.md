# NyaruDB2

**NyaruDB2** is a lightweight, high-performance database system built for mobile devices. It is designed to manage massive datasets (up to 1GB per shard) efficiently with features such as compression, partitioning (via shards), custom indexing, lazy query execution, detailed statistics, and benchmarking.

## Features

- **Sharded Storage**
  - Supports partitioning of data using a configurable shard key (e.g., `"category"`). Data is split across multiple shards to optimize I/O performance when handling large files.

- **Optimized Compression**
  - Supports various compression methods:
    - **none**: No compression.
    - **gzip**: Compression using libz.
    - **lzfse** and **lz4**: Compression using Apple’s [Compression](https://developer.apple.com/documentation/compression) framework for high performance.
    
- **Index Management**
  - Provides an `IndexManager` based on a B-Tree (implemented as an actor for concurrency safety) that supports custom index keys for fast lookups.

- **Query Engine**
  - Allows building flexible queries using multiple predicate types (equality, inequality, numeric comparisons, ranges, substring matching, and more). It supports lazy loading of documents using `AsyncThrowingStream` for efficient memory usage.

- **Statistics**
  - A `StatsEngine` compiles detailed per-collection and global statistics (number of shards, total documents, overall file size, and memory usage).

- **Benchmarking**
  - A benchmarking module is included to measure the performance of insertion, querying, updating, and deletion operations under different configurations (both partitioned and non-partitioned). It measures execution time, file sizes, and memory consumption, and outputs both a terminal report and detailed JSON files.

## Project Structure

```
NyaruDB2/
├── Package.swift
├── Sources/
│   ├── Benchmark/                # Benchmark executable
│   └── NyaruDB2/                 # Main framework source code
│       ├── Core/
│       │   ├── Commons/          # DynamicDecoder, FileProtection, etc.
│       │   ├── IndexManager/     # IndexManager and BTreeIndex implementations
│       │   ├── QueryEngine/      # QueryEngine for building and executing queries
│       │   ├── StatsEngine/      # Statistics collection
│       │   └── StorageEngine/    # StorageEngine, ShardManager, and Shard implementations
│       └── NyaruDB2.swift         # Public interface for the database
└── Tests/                        # Unit tests for each module
```

## Requirements

- Swift 5.9 or later
- Platforms: iOS 15+, macOS 12+ (others may work)

## Installation

Clone the repository and build the project using Swift Package Manager:

```bash
git clone https://github.com/your-username/NyaruDB2.git
cd NyaruDB2
swift build
```

## Running Tests

Execute all unit tests via:

```bash
swift test
```

## Usage

Integrate **NyaruDB2** into your project using Swift Package Manager. Here's a quick example of how to create and use the database:

```swift
import NyaruDB2

struct Person: Codable {
    let id: Int
    let name: String
    let category: String?
}

do {
    // Initialize NyaruDB2 with gzip compression and partition by "category"
    let db = try NyaruDB2(
        path: "/path/to/database",
        shardKey: "category",
        compressionMethod: .gzip,
        fileProtectionType: .none
    )
    
    // Create a sample document
    let person = Person(id: 1, name: "Alice", category: "A")
    
    // Insert the document into the "People" collection with an index on "name"
    try await db.insert(person, into: "People", indexField: "name")
    
    // Fetch all documents from the collection
    let people: [Person] = try await db.fetch(from: "People")
    print(people)
    
    // Update the document
    let updatedPerson = Person(id: 1, name: "Alice Updated", category: "A")
    try await db.update(updatedPerson, in: "People", matching: { $0.id == 1 }, indexField: "name")
    
    // Delete the document
    try await db.delete(where: { (p: Person) in p.name == "Alice Updated" }, from: "People")
    
    // Retrieve global statistics
    let stats = try await db.getGlobalStats()
    print("Total Collections: \(stats.totalCollections)")
    
} catch {
    print("Error: \(error)")
}
```

## Benchmarking

The **Benchmark** module (located in `Sources/Benchmark/Benchmark.swift`) allows you to run performance tests on NyaruDB2 under different configurations, measuring the performance of insert, query, update, and delete operations. It also reports file sizes and memory usage.

### Running the Benchmark

To run the benchmark from the command line:

```bash
swift run Benchmark
```

Benchmark results are printed to the terminal, and a detailed JSON report is saved in the user's document directory.

### Sample Benchmark Output

```
| Method        | Partition | Insert (s)   | Query (s)    | Update (s)   | Delete (s)   | File Size (MB) | Memory (MB) |
|---------------|-----------|--------------|--------------|--------------|--------------|----------------|-------------|
| none          | false     | 0.67         | 0.22         | 0.17         | 0.06         | 21.97          | 312         |
| none          | true      | 0.99         | 0.00         | 0.00         | 0.05         | 21.61          | 308         |
| gzip          | false     | 0.95         | 0.22         | 0.20         | 0.06         | 0.12           | 259         |
| gzip          | true      | 1.33         | 0.00         | 0.00         | 0.08         | 0.13           | 269         |
| lzfse         | false     | 0.93         | 0.22         | 0.20         | 0.06         | 0.09           | 254         |
| lzfse         | true      | 1.23         | 0.00         | 0.00         | 0.07         | 0.10           | 335         |
| lz4           | false     | 0.62         | 0.22         | 0.15         | 0.05         | 0.26           | 385         |
| lz4           | true      | 0.97         | 0.00         | 0.00         | 0.10         | 0.27           | 409         |
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with your improvements. Ensure your changes are accompanied by appropriate unit tests.

## License

This project is licensed under the [Apache License](LICENSE).

## Contact

For questions or suggestions, please contact: [demetrius.albuquerque@yahoo.com.br](mailto:demetrius.albuquerque@yahoo.com.br)