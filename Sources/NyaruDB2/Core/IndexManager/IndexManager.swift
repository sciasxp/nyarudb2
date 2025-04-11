import Foundation

public class IndexManager {
    private var indices: [String: BTreeIndex] = [:]
    
    public func createIndex(for field: String) {
        indices[field] = BTreeIndex()
    }
    
    public func search(_ field: String, value: AnyHashable) -> [Data] {
        return indices[field]?.search(value: value) ?? []
    }
}
