import Foundation

public struct CollectionMetadata: Codable {
    public let name: String
    public let indexes: [String]
    public let partitionKey: String

    public init(name: String, indexes: [String] = [], partitionKey: String) {
        self.name = name
        self.indexes = indexes
        self.partitionKey = partitionKey
    }
}

public class NDBCollection {
    public let metadata: CollectionMetadata
    private let db: NyaruDB2

    public init(db: NyaruDB2, name: String, indexes: [String] = [], partitionKey: String) {
        self.db = db
        self.metadata = CollectionMetadata(name: name, indexes: indexes, partitionKey: partitionKey)
    }

    public func insert<T: Codable>(_ document: T) async throws {

        let indexField = metadata.indexes.first
        try await db.insert(document, into: metadata.name, indexField: indexField)
    }

    public func bulkInsert<T: Codable>(_ documents: [T]) async throws {
        let indexField = metadata.indexes.first
        try await db.bulkInsert(documents, into: metadata.name, indexField: indexField)
    }

    public func findOne<T: Codable>(query: [String: Any]) async throws -> T? {

        let results: [T] = try await db.fetch(from: metadata.name)

        return results.first { document in
            // Converte o documento para dicionário para verificar os predicados.
            guard let data = try? JSONEncoder().encode(document),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            else { return false }
            // Validação simplificada: verifica se cada chave/valor do query está presente e igual no documento.
            for (key, value) in query {
                if let docValue = dict[key] as? String,
                   let queryValue = value as? String {
                    if docValue != queryValue { return false }
                } else if let docValue = dict[key] as? Int,
                          let queryValue = value as? Int {
                    if docValue != queryValue { return false }
                } else {
                    // Se não conseguir comparar, descarta esse documento.
                    return false
                }
            }
            return true
        }
    }

    public func findOne<T: Codable>(query: [String: Any], shardKey: String, shardValue: String) async throws -> T? {
        // Essa implementação é simplificada. Em um cenário real, o StorageEngine teria um método
        // para buscar documentos apenas no shard desejado.
        let results: [T] = try await db.fetch(from: metadata.name)
        let filtered = results.filter { document in
            guard let data = try? JSONEncoder().encode(document),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let value = dict[shardKey] as? String
            else { return false }
            return value == shardValue
        }
        // Aplica também os predicados gerais da query, de forma similar ao método anterior.
        return filtered.first { document in
            guard let data = try? JSONEncoder().encode(document),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            else { return false }
            for (key, value) in query {
                if let docValue = dict[key] as? String,
                   let queryValue = value as? String {
                    if docValue != queryValue { return false }
                } else if let docValue = dict[key] as? Int,
                          let queryValue = value as? Int {
                    if docValue != queryValue { return false }
                } else {
                    return false
                }
            }
            return true
        }
    }
    
    public func update<T: Codable>(_ document: T, matching predicate: @escaping (T) -> Bool) async throws {
        try await db.update(document, in: metadata.name, matching: predicate)
    }

    public func fetch<T: Codable>(
        query: [String: Any]? = nil,
        shardKey: String? = nil,
        shardValue: String? = nil
    ) async throws -> [T] {
        // Realiza o full scan na coleção utilizando o StorageEngine
        let results: [T] = try await db.fetch(from: metadata.name)
        
        // Filtra os resultados com base na shard, se os parâmetros forem informados
        let resultsFilteredByShard: [T] = {
            guard let shardKey = shardKey, let shardValue = shardValue else { return results }
            return results.filter { document in
                guard let data = try? JSONEncoder().encode(document),
                      let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let value = dict[shardKey] as? String
                else { return false }
                return value == shardValue
            }
        }()
        
        // Se nenhum query específico foi passado, retorna os resultados já filtrados por shard (se houver)
        guard let query = query else { return resultsFilteredByShard }
        
        // Filtra os resultados adicionais com base nos predicados do query.
        let finalResults = resultsFilteredByShard.filter { document in
            guard let data = try? JSONEncoder().encode(document),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            else { return false }
            for (key, value) in query {
                if let docValue = dict[key] as? String,
                   let queryValue = value as? String {
                    if docValue != queryValue { return false }
                } else if let docValue = dict[key] as? Int,
                          let queryValue = value as? Int {
                    if docValue != queryValue { return false }
                } else {
                    // Caso não seja possível comparar, descarta o documento.
                    return false
                }
            }
            return true
        }
        
        return finalResults
    }

    public func delete<T: Codable>(where predicate: @escaping (T) -> Bool) async throws {
        try await db.delete(where: predicate, from: metadata.name)
    }
}
