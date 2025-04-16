public enum FileProtectionType: String, CaseIterable {
    case none
    case complete
    case completeUnlessOpen
    case completeUntilFirstUserAuthentication

    public var systemValue: String {
        return "NS\(self.rawValue)"
    }
}
