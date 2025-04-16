import Foundation

public enum DynamicDecoder {

    /// Extrai o valor de uma chave específica dos dados JSON.
    /// - Parameters:
    ///   - data: Os dados JSON.
    ///   - key: A chave a ser extraída.
    ///   - forIndex: Se verdadeiro, lança um erro específico para índices.
    /// - Returns: O valor extraído como String.
    public static func extractValue(
        from data: Data,
        key: String,
        forIndex: Bool = false
    ) throws -> String {
        do {
            return try DynamicValueExtractor.extractValue(from: data, key: key)
        } catch _ as DecodingError {
            if forIndex {
                throw StorageEngine.StorageError.indexKeyNotFound(key)
            } else {
                throw StorageEngine.StorageError.partitionKeyNotFound(key)
            }
        } catch {
            throw error
        }
    }

    // MARK: - Implementação Interna
    private enum DynamicValueExtractor {

        enum ValueType: Decodable {
            case string(String)
            case number(NSNumber)
            case bool(Bool)
            case null

            init(from decoder: Decoder) throws {
                let container = try decoder.singleValueContainer()
                if let str = try? container.decode(String.self) {
                    self = .string(str)
                } else if let num = try? container.decode(Double.self) {
                    self = .number(NSNumber(value: num))
                } else if let bool = try? container.decode(Bool.self) {
                    self = .bool(bool)
                } else if container.decodeNil() {
                    self = .null
                } else {
                    throw DecodingError.dataCorruptedError(
                        in: container,
                        debugDescription: "Unsupported value type"
                    )
                }
            }
        }

        static func extractValue(from data: Data, key: String) throws -> String
        {
            let decoder = JSONDecoder()
            let dict = try decoder.decode([String: ValueType].self, from: data)
            guard let value = dict[key] else {
                throw DecodingError.keyNotFound(
                    DynamicCodingKey(stringValue: key)!,
                    DecodingError.Context(
                        codingPath: [],
                        debugDescription: "Key \(key) not found"
                    )
                )
            }
            switch value {
            case .string(let str):
                return str
            case .number(let num):
                return num.stringValue
            case .bool(let bool):
                return bool ? "true" : "false"
            case .null:
                return "null"
            }
        }
    }

    private struct DynamicCodingKey: CodingKey {
        var stringValue: String
        var intValue: Int? { return nil }

        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { nil }
    }
}
