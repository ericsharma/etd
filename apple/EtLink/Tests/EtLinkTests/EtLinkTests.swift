import Foundation
import XCTest
@testable import EtLink

final class EtLinkTests: XCTestCase {
    func testOfflineAPI() throws {
        XCTAssertEqual(generateSecretKey().count, 32)
        XCTAssertThrowsError(try parseTicket(url: "https://example.com"))
    }

    func testLinkStarts() async throws {
        let start = Date()
        let link = try await Link.start(secretKey: generateSecretKey())
        print("Link.start took \(Date().timeIntervalSince(start)) s")
        XCTAssertEqual(link.endpointId().count, 64)
        await link.shutdown()
    }

    /// End to end against a real etd. Set ET_PAIR_URL to a fresh
    /// `etd pair --url` (valid 10 minutes, one use). With xcodebuild, pass
    /// it as TEST_RUNNER_ET_PAIR_URL. Only reads: exec `echo`, and
    /// `display-message` in the first tmux session.
    func testEndToEnd() async throws {
        guard let url = ProcessInfo.processInfo.environment["ET_PAIR_URL"], !url.isEmpty else {
            throw XCTSkip("set ET_PAIR_URL to run")
        }
        let info = try parseTicket(url: url)
        let link = try await Link.start(secretKey: generateSecretKey())
        let paired = try await link.pair(url: url, deviceName: "etlink-test")
        XCTAssertEqual(paired.endpointId, info.endpointId)

        let host = try await link.connect(endpointId: paired.endpointId, relayUrl: paired.relayUrl)
        let hello = try await host.hello(app: "EtLinkTests")
        XCTAssertNotNil(hello.tmux, hello.tmuxError ?? "")
        print("host \(hello.hostName), \(hello.os), etd \(hello.etdVersion), tmux \(hello.tmux?.version ?? "-")")

        let exec = try await host.exec(script: "echo etlink-ok")
        XCTAssertEqual(String(decoding: exec.output, as: UTF8.self), "etlink-ok\n")
        XCTAssertEqual(exec.exitCode, 0)

        let sessions = try await host.listSessions()
        if let first = sessions.first {
            let control = try await host.openControl(target: .attach(name: first.name))
            try await control.write(bytes: Data("display-message -p etlink-ok\n".utf8))
            var seen = ""
            while !seen.contains("etlink-ok") {
                if case .stdout(let bytes) = try await control.next() {
                    seen += String(decoding: bytes, as: UTF8.self)
                }
            }
            await control.finish()
            while true {
                if case .exit = try await control.next() { break }
            }
        }

        let ping = try await host.ping()
        let path = host.path()
        print("path \(path.kind), rtt \(path.rttMs) ms, ping \(ping) ms")
        host.close()
        await link.shutdown()
    }
}
