public struct NyaruDB2 {
    public let storage: StorageEngine
    public let indexManager: IndexManager
    
    public init(path: String = "NyaruDB2") throws {
        self.storage = try StorageEngine(path: path)
        self.indexManager = IndexManager()
    }
}
