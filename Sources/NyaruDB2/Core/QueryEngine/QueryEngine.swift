public struct Query<T: Codable> {
    public enum Operator {
        case equal, greaterThan, contains
    }
    
    private var predicates: [(field: String, op: Operator, value: Any)] = []
    
    public func `where`(_ field: String, _ op: Operator, _ value: Any) -> Self {
        var copy = self
        copy.predicates.append((field, op, value))
        return copy
    }
    
    public func fetch(from storage: StorageEngine) async throws -> [T] {
        return []
    }
}
