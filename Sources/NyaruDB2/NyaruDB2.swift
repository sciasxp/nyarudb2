public struct NyaruDB2 {
    public let storage: StorageEngine
    public let indexManager: IndexManager
    
    public init(path: String = "NyaruDB2") {
        self.storage = StorageEngine(path: path)
        self.indexManager = IndexManager()
    }
}
