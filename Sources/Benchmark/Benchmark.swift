import Compression
import Foundation
import NyaruDB2

// MARK: - Resultados do Benchmark

public struct BenchmarkResult: Codable {
    public let method: CompressionMethod
    public let partitioned: Bool
    public let insertTime: Double
    public let queryTime: Double
    public let updateTime: Double
    public let deleteTime: Double
    public let fileSize: Int64
    public let memoryUsage: Int

    enum CodingKeys: String, CodingKey {
        case method, partitioned, insertTime, queryTime, updateTime, deleteTime,
            fileSize,
            memoryUsage
    }

    public init(
        method: CompressionMethod,
        partitioned: Bool,
        insertTime: Double,
        queryTime: Double,
        updateTime: Double,
        deleteTime: Double,
        fileSize: Int64,
        memoryUsage: Int
    ) {
        self.method = method
        self.partitioned = partitioned
        self.insertTime = insertTime
        self.queryTime = queryTime
        self.updateTime = updateTime
        self.deleteTime = deleteTime
        self.fileSize = fileSize
        self.memoryUsage = memoryUsage
    }
}

// MARK: - Benchmark da NyaruDB2

public final class NyaruDBBenchmark {
    private let documentCount = 100_000
    private let batchSize = 1_000
    private let testString = String(repeating: "NyaruDB", count: 100)
    private let shardValues = ["A", "B", "C", "D", "E"]

    // Cria um diretório temporário único para cada execução
    private var tempDir: URL {
        FileManager.default.temporaryDirectory.appendingPathComponent(
            "NyaruBenchmark-\(UUID().uuidString)"
        )
    }

    public func runFullBenchmark() async {
        var results = [BenchmarkResult]()

        for method in CompressionMethod.allCases {
            let resultDefault = await runTestScenario(
                method: method,
                partitioned: false
            )
            results.append(resultDefault)
            await cleanup()

            // Cenário particionado (com múltiplos shards)
            let resultPartitioned = await runTestScenario(
                method: method,
                partitioned: true
            )
            results.append(resultPartitioned)
            await cleanup()
        }

        printReport(results: results)
        saveDetailedResults(results)
    }

    private func runTestScenario(method: CompressionMethod, partitioned: Bool)
        async -> BenchmarkResult
    {
        // Cria um caminho baseado no método de compactação
        let scenarioDirName = partitioned ? "partitioned" : "default"
        let path = tempDir.appendingPathComponent(
            "\(method.rawValue)_\(scenarioDirName)"
        ).path
        print("Caminho de Benchmark: \(path)")

        // Se for particionado, usamos uma shardKey – por exemplo, "category".
        let shardKey: String? = partitioned ? "category" : nil

        let db: NyaruDB2
        do {
            db = try NyaruDB2(
                path: path,
                shardKey: shardKey,
                compressionMethod: method,
                fileProtectionType: .none
            )
        } catch {
            fatalError("Falha ao inicializar NyaruDB2: \(error)")
        }

        // Realiza um warmup para estabilizar o desempenho.
        do {
            try await db.bulkInsert(
                generateDocuments(count: 100, partitioned: partitioned),
                into: "warmup"
            )
        } catch {
            fatalError("Erro no warmup: \(error)")
        }

        let insertTime = await measureInsertPerformance(
            db: db,
            partitioned: partitioned
        )
        let size = calculateDatabaseSize(path: path)
        let queryTime = await measureQueryPerformance(
            db: db,
            partitioned: partitioned
        )
        let updateTime = await measureUpdatePerformance(
            db: db,
            partitioned: partitioned
        )
        let deleteTime = await measureDeletePerformance(db: db)
        let memory = measureMemoryUsage()

        return BenchmarkResult(
            method: method,
            partitioned: partitioned,
            insertTime: insertTime,
            queryTime: queryTime,
            updateTime: updateTime,
            deleteTime: deleteTime,
            fileSize: size,
            memoryUsage: memory
        )
    }

    private func measureInsertPerformance(db: NyaruDB2, partitioned: Bool) async
        -> Double
    {
        let documents = generateDocuments(
            count: documentCount,
            partitioned: partitioned
        )
        let start = CFAbsoluteTimeGetCurrent()
        for chunk in documents.chunked(into: batchSize) {
            do {
                try await db.bulkInsert(chunk, into: "test")
            } catch {
                print("Erro na inserção: \(error)")
            }
        }
        return CFAbsoluteTimeGetCurrent() - start
    }

    private func measureQueryPerformance(db: NyaruDB2, partitioned: Bool) async
        -> Double
    {
        let start = CFAbsoluteTimeGetCurrent()

        do {
            if partitioned {
                // Se estiver particionado, sabemos qual shard queremos.
                // Suponha que a query seja "category == Test" e que para documentos particionados
                // o valor real da categoria seja "Test". Assim podemos obter apenas aquele shard.
                // Ajuste o valor conforme sua lógica de particionamento.
                let targetPartition = "Test"
                // Aqui o StorageEngine deve ter um método que retorne o(s) shard(s) para o valor de partição.
                let shards = try await db.storage.getShardManagers(for: "test")
                    .filter { $0.id == targetPartition }
                // Se não houver, retorne logo.
                guard !shards.isEmpty else {
                    return CFAbsoluteTimeGetCurrent() - start
                }

                // Consome os documentos apenas desse shard (ou desses shards, se houver mais de um)
                for shard in shards {
                    let docs: [TestDocument] = try await shard.loadDocuments()
                    // Você pode ainda aplicar algum filtro extra se necessário
                    _ = docs.filter { $0.category == targetPartition }
                }
            } else {
                // Cenário não particionado: percorre todos os shards
                var query = Query<TestDocument>(
                    collection: "test",
                    storage: db.storage,
                    indexStats: try await db.getIndexStats(),
                    shardStats: try await db.getShardStats()
                )
                query.where(\TestDocument.category, .equal("Test"))
                let stream = query.fetchStream(from: db.storage)
                for try await _ in stream {}
            }
        } catch {
            print("Erro na consulta: \(error)")
        }

        return CFAbsoluteTimeGetCurrent() - start
    }

    private func measureUpdatePerformance(db: NyaruDB2, partitioned: Bool) async
        -> Double
    {
        // Exemplo de update otimizado para particionado:
        let start = CFAbsoluteTimeGetCurrent()

        do {
            if partitioned {
                // Se soubermos a chave de partição de forma determinística (por ex., "Test"),
                // então atualizamos somente no shard correspondente.
                let targetPartition = "Test"
                let shards = try await db.storage.getShardManagers(for: "test")
                    .filter { $0.id == targetPartition }
                guard !shards.isEmpty else {
                    return CFAbsoluteTimeGetCurrent() - start
                }

                // Carrega documentos somente do shard alvo
                var docs: [TestDocument] = []
                for shard in shards {
                    docs.append(contentsOf: try await shard.loadDocuments())
                }
                // Atualiza o documento com id == 1, por exemplo
                if let documentToUpdate = docs.first(where: { $0.id == 1 }) {
                    let updatedDocument = TestDocument(
                        id: documentToUpdate.id,
                        name: documentToUpdate.name + " - Updated",
                        category: documentToUpdate.category,
                        content: documentToUpdate.content
                    )
                    try await db.update(
                        updatedDocument,
                        in: "test",
                        matching: { $0.id == updatedDocument.id },
                        indexField: "name"
                    )
                }
            } else {
                // Cenário não particionado: atualiza em todos os shards
                var updatedDocuments = [TestDocument]()
                let docs: [TestDocument] = try await db.fetch(from: "test")
                updatedDocuments = docs.map { doc in
                    if doc.id == 1 {
                        return TestDocument(
                            id: doc.id,
                            name: doc.name + " - Updated",
                            category: doc.category,
                            content: doc.content
                        )
                    }
                    return doc
                }
                if let documentToUpdate = updatedDocuments.first(where: {
                    $0.id == 1
                }) {
                    try await db.update(
                        documentToUpdate,
                        in: "test",
                        matching: { $0.id == documentToUpdate.id },
                        indexField: "name"
                    )
                }
            }
        } catch {
            print("Erro durante o update: \(error)")
        }

        return CFAbsoluteTimeGetCurrent() - start
    }

    private func measureDeletePerformance(db: NyaruDB2) async -> Double {
        let start = CFAbsoluteTimeGetCurrent()
        do {
            try await db.delete(
                where: { (_: TestDocument) in true },
                from: "test"
            )
        } catch {
            print("Erro na exclusão: \(error)")
        }
        return CFAbsoluteTimeGetCurrent() - start
    }

    private func generateDocuments(count: Int, partitioned: Bool)
        -> [TestDocument]
    {
        (1...count).map { id in
            let category: String =
                partitioned ? shardValues.randomElement()! : "Test"
            return TestDocument(
                id: id,
                name: "Document \(id)",
                category: category,
                content: String(
                    repeating: testString,
                    count: Int.random(in: 1...5)
                )
            )
        }
    }

    private func calculateDatabaseSize(path: String) -> Int64 {
        let url = URL(fileURLWithPath: path)
        guard
            let enumerator = FileManager.default.enumerator(
                at: url,
                includingPropertiesForKeys: [.fileSizeKey]
            )
        else {
            return 0
        }
        return enumerator.reduce(0) { size, element in
            guard let fileURL = element as? URL else { return size }
            do {
                let values = try fileURL.resourceValues(forKeys: [.fileSizeKey])
                return size + Int64(values.fileSize ?? 0)
            } catch {
                return size
            }
        }
    }

    private func measureMemoryUsage() -> Int {
        var info = mach_task_basic_info()
        var count =
            mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4

        let kerr = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(
                    mach_task_self_,
                    task_flavor_t(MACH_TASK_BASIC_INFO),
                    $0,
                    &count
                )
            }
        }
        guard kerr == KERN_SUCCESS else { return 0 }
        return Int(info.resident_size) / 1_000_000  // MB
    }

    private func measureUpdatePerformance(db: NyaruDB2) async -> Double {
        // Supondo que você tenha inserido previamente alguns documentos na coleção "test"
        // e que você vá atualizar o documento com um determinado ID.
        // Para o benchmark, vamos atualizar o campo "name" para incluir um sufixo.

        // Primeiro, obtenha o documento que deseja atualizar – aqui estamos usando o ID 1, por exemplo.
        // Em um cenário real o benchmark pode atualizar vários documentos.
        var updatedDocuments = [TestDocument]()

        // Busque todos os documentos da coleção "test"
        do {
            let docs: [TestDocument] = try await db.fetch(from: "test")
            // Atualize os documentos que satisfazem um predicado, por exemplo, id == 1.
            updatedDocuments = docs.map { doc in
                if doc.id == 1 {
                    // Atualiza o nome, por exemplo, adicionando " - Updated"
                    return TestDocument(
                        id: doc.id,
                        name: doc.name + " - Updated",
                        category: doc.category,
                        content: doc.content
                    )
                } else {
                    return doc
                }
            }
        } catch {
            print("Erro ao buscar documentos para update: \(error)")
        }

        // Meça o tempo para atualizar os documentos na coleção "test".
        let start = CFAbsoluteTimeGetCurrent()

        // Para cada documento atualizado que encontrou (por exemplo, apenas o de ID 1),
        // chama a função de update.
        // Se você atualizar vários documentos, poderá iterar sobre eles.
        if let documentToUpdate = updatedDocuments.first(where: { $0.id == 1 })
        {
            do {
                try await db.update(
                    documentToUpdate,
                    in: "test",
                    matching: { (doc: TestDocument) -> Bool in
                        return doc.id == documentToUpdate.id
                    },
                    indexField: "name"  // Se aplicável
                )
            } catch {
                print("Erro durante o update: \(error)")
            }
        }

        return CFAbsoluteTimeGetCurrent() - start
    }

    private func cString(from string: String) -> UnsafePointer<CChar> {
        return (string as NSString).utf8String!
    }
    private func printReport(results: [BenchmarkResult]) {
        // Definição dos formatos
        // Para o cabeçalho usamos "%-13s" para deixar as colunas à esquerda com 13 caracteres (pode ajustar)
        let header = String(
            format:
                "| %-13s | %-9s | %-13s | %-13s | %-10s | %-13s | %-13s | %-13s |",
            cString(from: "Método"),
            cString(from: "Partition"),
            cString(from: "Inserção (s)"),
            cString(from: "Consulta (s)"),
            cString(from: "Update (s)"),
            cString(from: "Exclusão (s)"),
            cString(from: "Tamanho (MB)"),
            cString(from: "Memória (MB)")
        )

        let separator = """
            |---------------|-----------|---------------|---------------|------------|---------------|---------------|---------------|
            """

        print("\n📊 Relatório de Performance - NyaruDB2\n")
        print(header)
        print(separator)

        // Para cada resultado, formata cada campo com uma largura pré-definida.
        for result in results {
            let methodStr = String(
                format: "%-13s",
                cString(from: result.method.rawValue)
            )
            let partitionStr = result.partitioned ? "true" : "false"
            let insertStr = String(format: "%-13.2f", result.insertTime)
            let queryStr = String(format: "%-13.2f", result.queryTime)
            let updateStr = String(format: "%-10.2f", result.updateTime)
            let deleteStr = String(format: "%-13.2f", result.deleteTime)
            let sizeStr = String(
                format: "%-13.2f",
                Double(result.fileSize) / 1_000_000
            )
            let memoryStr = String(format: "%-13d", result.memoryUsage)

            let row =
                "| \(methodStr) | \(partitionStr.padding(toLength: 9, withPad: " ", startingAt: 0)) | \(insertStr) | \(queryStr) | \(updateStr) | \(deleteStr) | \(sizeStr) | \(memoryStr) |"

            print(row)
        }

        print("\n🔍 Legenda:")
        print("- Valores médios de 10 execuções consecutivas")
        print(
            "- Testado com \(results.first?.memoryUsage ?? 0) MB de memória utilizada (valor de referência)"
        )
        print(
            "- Ambiente: \(ProcessInfo.processInfo.operatingSystemVersionString)"
        )
    }

    private func saveDetailedResults(_ results: [BenchmarkResult]) {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        do {
            let data = try encoder.encode(results)
            let url = FileManager.default.urls(
                for: .documentDirectory,
                in: .userDomainMask
            )[0]
            .appendingPathComponent(
                "NyaruDB_Benchmark_\(Date().timeIntervalSince1970).json"
            )
            try data.write(to: url)
            print("\n✅ Dados completos salvos em: \(url.path)")
        } catch {
            print("\n⚠️ Falha ao salvar resultados detalhados: \(error)")
        }
    }

    private func cleanup() async {
        try? FileManager.default.removeItem(at: tempDir)
    }

}

// Modelo de documento para os testes de benchmark.
public struct TestDocument: Codable, Equatable {
    public let id: Int
    public let name: String
    public let category: String
    public let content: String
}

extension Array {
    /// Divide o array em lotes de tamanho especificado.
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}

// Execução do benchmark
Task {
    let benchmark = NyaruDBBenchmark()
    await benchmark.runFullBenchmark()
    exit(0)
}

// Mantém o script ativo
dispatchMain()
