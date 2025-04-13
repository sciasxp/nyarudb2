import Foundation

public struct Query<T: Codable> {
    
    public enum Operator {
        case equal(Any)
        case notEqual(Any)
        case lessThan(Any)
        case lessThanOrEqual(Any)
        case greaterThan(Any)
        case greaterThanOrEqual(Any)
        case between(lower: Any, upper: Any)
        case contains(Any)
        case startsWith(String)
        case endsWith(String)
        case `in`([Any])
        case exists
        case notExists
    }
    
    private let collection: String
    private var predicates: [(field: String, op: Operator)] = []
    
    public init(collection: String) {
        self.collection = collection
    }
    
    /// Adiciona um predicado à query. Exemplo:
    ///     query.where("age", .greaterThan(18))
    public func `where`(_ field: String, _ op: Operator) -> Self {
        var copy = self
        copy.predicates.append((field, op))
        return copy
    }
    
    // MARK: - Lazy Query via AsyncThrowingStream
    
    /// Retorna um fluxo assíncrono (lazy) de documentos que atendem aos predicados da query.
    /// Essa função usa AsyncThrowingStream para emitir documentos conforme eles são filtrados.
    public func fetchStream(from storage: StorageEngine) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    // Carrega os shards da coleção. Suponha que o StorageEngine ofereça esse método.
                    let shards = try await storage.getShardManagers(for: collection)
                    
                    for shard in shards {
                        let docs: [T] = try await shard.loadDocuments()
                        for doc in docs {
                            let dict = try convertToDictionary(doc)
                            var satisfies = true
                            for (field, op) in predicates {
                                let fieldValue = dict[field]
                                if !evaluatePredicate(documentValue: fieldValue, op: op) {
                                    satisfies = false
                                    break
                                }
                            }
                            if satisfies {
                                continuation.yield(doc)
                            }
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }
    
    // MARK: - Helper para converter um objeto para dicionário
    private func convertToDictionary(_ value: T) throws -> [String: Any] {
        let data = try JSONEncoder().encode(value)
        let obj = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = obj as? [String: Any] else {
            throw NSError(
                domain: "QueryEngine",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Falha ao converter objeto para dicionário"]
            )
        }
        return dict
    }
    
    // MARK: - Avaliação dos Predicados
    private func evaluatePredicate(documentValue: Any?, op: Operator) -> Bool {
        switch op {
        case .exists:
            return documentValue != nil
        case .notExists:
            return documentValue == nil
        case .equal(let target):
            return compareEquality(documentValue, target)
        case .notEqual(let target):
            return !compareEquality(documentValue, target)
        case .lessThan(let target):
            return compareNumeric(documentValue, target, using: <)
        case .lessThanOrEqual(let target):
            return compareNumeric(documentValue, target, using: <=)
        case .greaterThan(let target):
            return compareNumeric(documentValue, target, using: >)
        case .greaterThanOrEqual(let target):
            return compareNumeric(documentValue, target, using: >=)
        case .between(let lower, let upper):
            return compareNumeric(documentValue, lower, using: >=) && compareNumeric(documentValue, upper, using: <=)
        case .contains(let target):
            if let s1 = stringValue(documentValue), let s2 = stringValue(target) {
                return s1.contains(s2)
            }
            return false
        case .startsWith(let prefix):
            if let s1 = stringValue(documentValue) {
                return s1.hasPrefix(prefix)
            }
            return false
        case .endsWith(let suffix):
            if let s1 = stringValue(documentValue) {
                return s1.hasSuffix(suffix)
            }
            return false
        case .in(let array):
            if let s1 = stringValue(documentValue) {
                for elem in array {
                    if s1 == stringValue(elem) {
                        return true
                    }
                }
            }
            return false
        }
    }
    
    // Compara igualdade tentando valores numéricos antes de usar a comparação textual.
    private func compareEquality(_ value1: Any?, _ value2: Any) -> Bool {
        if let d1 = toDouble(value1), let d2 = toDouble(value2) {
            return d1 == d2
        }
        if let s1 = stringValue(value1), let s2 = stringValue(value2) {
            return s1 == s2
        }
        return false
    }
    
    // Tenta converter o valor para Double para comparações numéricas.
    private func toDouble(_ value: Any?) -> Double? {
        if let num = value as? NSNumber {
            return num.doubleValue
        }
        if let str = value as? String, let d = Double(str) {
            return d
        }
        if let d = value as? Double {
            return d
        }
        if let i = value as? Int {
            return Double(i)
        }
        return nil
    }
    
    // Converte um valor para String, se possível.
    private func stringValue(_ value: Any?) -> String? {
        if let value = value {
            return "\(value)"
        }
        return nil
    }
    
    // Para comparações numéricas: tenta comparar os valores convertidos em Double.
    private func compareNumeric(_ value1: Any?, _ value2: Any, using comparator: (Double, Double) -> Bool) -> Bool {
        if let d1 = toDouble(value1), let d2 = toDouble(value2) {
            return comparator(d1, d2)
        }
        // Se não for numérico, tenta comparar como string (ordenadas lexicograficamente)
        if let s1 = stringValue(value1), let s2 = stringValue(value2) {
            return comparator(Double(s1.hashValue), Double(s2.hashValue))
        }
        return false
    }
}
