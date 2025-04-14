// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "NyaruDB2",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [
        .library(
            name: "NyaruDB2",
            targets: ["NyaruDB2"]),
    ],
    targets: [
        .target(
            name: "NyaruDB2",
            path: "Sources/NyaruDB2",
            swiftSettings: [
                            .unsafeFlags(["-warnings-as-errors"]), // Trata warnings como erros
                            .define("IOS15_8_OR_LATER") // Define um flag para versão mínima
                        ],
            linkerSettings: [
                .linkedLibrary("z"),  // Necessário para GZIP
                .linkedFramework("Compression", .when(platforms: [.iOS]))
            ]
        ),
        .executableTarget(
            name: "Benchmark",
            dependencies: ["NyaruDB2"],
            path: "Sources/Benchmark"
        ),
        .testTarget(
            name: "NyaruDB2Tests",
            dependencies: ["NyaruDB2"],
            path: "Tests/NyaruDB2Tests"
        ),
    ]
)
