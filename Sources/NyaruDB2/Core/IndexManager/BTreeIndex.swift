import Foundation

public class BTreeIndex {
    private var tree: [AnyHashable: [Data]] = [:]
    
    public func insert(key: AnyHashable, data: Data) {
        tree[key, default: []].append(data)
    }
    
    public func search<T: Hashable>(value: T) -> [Data] {
        return tree[AnyHashable(value)] ?? []
    }
}
