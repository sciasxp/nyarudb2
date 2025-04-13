public enum FileProtectionType: String {
    case none = "NSFileProtectionNone"
    case complete = "NSFileProtectionComplete"
    case completeUnlessOpen = "NSFileProtectionCompleteUnlessOpen"
    case completeUntilFirstUserAuthentication = "NSFileProtectionCompleteUntilFirstUserAuthentication"
}
