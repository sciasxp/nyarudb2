/// An enumeration representing the types of file protection available.
/// 
/// This enum provides different levels of file protection that can be applied
/// to secure files. Each case corresponds to a specific file protection type
/// and is represented as a `String`.
///
/// - Conforms to:
///   - `CaseIterable`: Allows iteration over all cases of the enum.
public enum FileProtectionType: String, CaseIterable {
    case none
    case complete
    case completeUnlessOpen
    case completeUntilFirstUserAuthentication

    public var systemValue: String {
        return "NS\(self.rawValue)"
    }
}
