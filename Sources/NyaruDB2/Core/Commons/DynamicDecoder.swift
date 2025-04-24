import Foundation

/// An enumeration that provides functionality for dynamic decoding of data.
/// This can be used to decode data whose structure is not known at compile time.
public enum DynamicDecoder {

    
    /// Extracts a value from the given data for a specified key.
    ///
    /// - Parameters:
    ///   - data: The `Data` object containing the encoded information.
    ///   - key: The key whose associated value needs to be extracted.
    ///   - forIndex: A Boolean value indicating whether the extraction is for indexing purposes. Defaults to `false`.
    /// - Returns: A `String` representing the extracted value.
    /// - Throws: An error if the extraction process fails.
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

    /// An enumeration that provides functionality to extract dynamic values.
    /// This is used internally to handle dynamic decoding of values.
    private enum DynamicValueExtractor {

        /// An enumeration representing different types of values that can be decoded.
        /// 
        /// - Cases:
        ///   - `string(String)`: Represents a string value.
        ///   - `number(NSNumber)`: Represents a numeric value.
        ///   - `bool(Bool)`: Represents a boolean value.
        ///   - `null`: Represents a null value.
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

        /// Extracts a value associated with a given key from the provided data.
        /// 
        /// This method decodes the data to retrieve the value corresponding to the specified key.
        /// 
        /// - Parameters:
        ///   - data: The `Data` object containing the encoded information.
        ///   - key: The key for which the associated value is to be extracted.
        /// - Returns: A `String` representing the value associated with the given key.
        /// - Throws: An error if the decoding process fails or the key is not found.
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

    /// A private struct that conforms to the `CodingKey` protocol.
    /// This is used to dynamically create coding keys at runtime,
    /// enabling flexible decoding of data structures where the keys
    /// are not known at compile time.
    private struct DynamicCodingKey: CodingKey {
        var stringValue: String
        var intValue: Int? { return nil }

        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { nil }
    }
}
