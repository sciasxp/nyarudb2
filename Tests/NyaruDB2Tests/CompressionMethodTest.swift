//
//  CompressionMethodTest.swift

//  NyaruDB2
//
//  Created by demetrius albuquerque on 11/04/25.
//

import XCTest

@testable import NyaruDB2

final class CompressionMethodTests: XCTestCase {

    func testGzipRoundTrip() throws {
        let originalString = "Teste de compressão GZIP com dados de exemplo."
        guard let originalData = originalString.data(using: .utf8) else {
            XCTFail("Não foi possível criar os dados a partir da string")
            return
        }

        // Comprime usando GZIP
        let compressedData = try compressData(originalData, method: .gzip)
        // Comprime deve ser diferente dos dados originais (na maioria dos casos)
        XCTAssertNotEqual(
            originalData,
            compressedData,
            "Dados comprimidos devem ser diferentes dos originais"
        )

        // Descomprime e verifica se volta ao original
        let decompressedData = try decompressData(compressedData, method: .gzip)
        XCTAssertEqual(
            originalData,
            decompressedData,
            "Dados descomprimidos devem ser iguais aos originais"
        )
    }

    func testLZFSERoundTrip() throws {
        let originalData = "Outro teste para LZFSE".data(using: .utf8)!
        let compressedData = try compressData(originalData, method: .lzfse)
        let decompressedData = try decompressData(
            compressedData,
            method: .lzfse
        )
        XCTAssertEqual(originalData, decompressedData)
    }

    func testLZ4RoundTrip() throws {
        let originalData = "Teste para LZ4, dados de exemplo".data(
            using: .utf8
        )!
        let compressedData = try compressData(originalData, method: .lz4)
        let decompressedData = try decompressData(compressedData, method: .lz4)
        XCTAssertEqual(originalData, decompressedData)
    }
}
