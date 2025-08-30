// swift-tools-version:5.7
import PackageDescription

let package = Package(
  name: "SyncEngine",
  platforms: [
    .iOS(.v16)
  ],
  products: [
    .library(name: "SyncEngine", targets: ["SyncEngine"]) // expose Swift wrapper
  ],
  targets: [
    .binaryTarget(
      name: "SyncEngineCore",
      url: "https://github.com/Dominik7787/Sync-Engine-Client-Side/releases/download/v0.1.0/SyncEngine.xcframework.zip",
      checksum: "6a2d19d48ca9bce19523d22893ea65cd609ca6bf54e66b37ed790aa589448c1d"
    ),
    .target(
      name: "SyncEngine",
      dependencies: ["SyncEngineCore"],
      path: "swift/Sources/SyncEngine"
    )
  ]
)


