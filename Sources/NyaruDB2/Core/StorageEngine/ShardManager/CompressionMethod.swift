import Foundation
import Compression
import zlib

public enum CompressionMethod {
    case none
    case gzip
    case lzfse
    case lz4
}

public enum CompressionError: Error {
    case compressionFailed
    case decompressionFailed
    case unsupportedMethod
    case zlibError(code: Int)
}

// MARK: - Funções Públicas

public func compressData(_ data: Data, method: CompressionMethod) throws -> Data {
    guard !data.isEmpty else { return data }
    switch method {
    case .none:
        return data
    case .gzip:
        return try gzipCompress(data: data)
    case .lzfse:
        return try compress(data: data, algorithm: COMPRESSION_LZFSE)
    case .lz4:
        return try compress(data: data, algorithm: COMPRESSION_LZ4)
    }
}

public func decompressData(_ data: Data, method: CompressionMethod) throws -> Data {
    guard !data.isEmpty else { return data }
    switch method {
    case .none:
        return data
    case .gzip:
        return try gzipDecompress(data: data)
    case .lzfse:
        return try decompress(data: data, algorithm: COMPRESSION_LZFSE)
    case .lz4:
        return try decompress(data: data, algorithm: COMPRESSION_LZ4)
    }
}

// MARK: - Helpers com Compression Framework (LZFSE, LZ4)

private func compress(data: Data, algorithm: compression_algorithm) throws -> Data {
    // Aloca um buffer de destino com tamanho estimado
    let dstBufferSize = data.count + (data.count / 10) + 100
    let destinationBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: dstBufferSize)
    defer { destinationBuffer.deallocate() }
    
    
    var stream = compression_stream(
        dst_ptr: destinationBuffer,
        dst_size: dstBufferSize,
        src_ptr: (data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count),
        src_size: data.count,
        state: nil
    )

    // Inicializa o stream para codificação (compressão)
    let statusInit = compression_stream_init(&stream, COMPRESSION_STREAM_ENCODE, algorithm)
    guard statusInit == COMPRESSION_STATUS_OK else {
        throw CompressionError.compressionFailed
    }
    defer { compression_stream_destroy(&stream) }
    
    var outputData = Data()
    // Usa withUnsafeBytes para acessar os dados de forma segura
    try data.withUnsafeBytes { (srcPointer: UnsafeRawBufferPointer) in
        guard let baseAddress = srcPointer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
            throw CompressionError.compressionFailed
        }
        
        stream.src_ptr = baseAddress
        stream.src_size = data.count
        stream.dst_ptr = destinationBuffer
        stream.dst_size = dstBufferSize
        
        while true {
            let flag = Int32(COMPRESSION_STREAM_FINALIZE.rawValue)
            let status = compression_stream_process(&stream, flag)
            switch status {
            case COMPRESSION_STATUS_OK, COMPRESSION_STATUS_END:
                // Calcula quantos bytes foram escritos no buffer
                let count = dstBufferSize - stream.dst_size
                if count > 0 {
                    outputData.append(destinationBuffer, count: count)
                }
                if status == COMPRESSION_STATUS_END {
                    return
                }
                // Reseta o buffer de destino para a próxima rodada
                stream.dst_ptr = destinationBuffer
                stream.dst_size = dstBufferSize
            default:
                throw CompressionError.compressionFailed
            }
        }
    }
    return outputData
}

private func decompress(data: Data, algorithm: compression_algorithm) throws -> Data {
    // Estimativa do tamanho do buffer de destino (em geral, o resultado descompactado pode ser maior)
    let dstBufferSize = data.count * 4
    let destinationBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: dstBufferSize)
    defer { destinationBuffer.deallocate() }
    

    var stream = compression_stream(
        dst_ptr: destinationBuffer,
        dst_size: dstBufferSize,
        src_ptr: (data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count),
        src_size: data.count,
        state: nil
    )

    let statusInit = compression_stream_init(&stream, COMPRESSION_STREAM_DECODE, algorithm)
    guard statusInit == COMPRESSION_STATUS_OK else {
        throw CompressionError.decompressionFailed
    }
    defer { compression_stream_destroy(&stream) }
    
    var outputData = Data()
    try data.withUnsafeBytes { (srcPointer: UnsafeRawBufferPointer) in
        guard let baseAddress = srcPointer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
            throw CompressionError.decompressionFailed
        }
        
        stream.src_ptr = baseAddress
        stream.src_size = data.count
        stream.dst_ptr = destinationBuffer
        stream.dst_size = dstBufferSize
        
        while true {
            let status = compression_stream_process(&stream, 0)
            switch status {
            case COMPRESSION_STATUS_OK, COMPRESSION_STATUS_END:
                let count = dstBufferSize - stream.dst_size
                if count > 0 {
                    outputData.append(destinationBuffer, count: count)
                }
                if status == COMPRESSION_STATUS_END {
                    return
                }
                stream.dst_ptr = destinationBuffer
                stream.dst_size = dstBufferSize
            default:
                throw CompressionError.decompressionFailed
            }
        }
    }
    return outputData
}

// MARK: - Implementação GZIP utilizando libz

private func gzipCompress(data: Data) throws -> Data {
    var stream = z_stream()
    var status: Int32 = deflateInit2_(
        &stream,
        Z_DEFAULT_COMPRESSION,
        Z_DEFLATED,
        15 + 16,    // 15 bits + 16 para o header GZIP
        8,          // nível de memória
        Z_DEFAULT_STRATEGY,
        ZLIB_VERSION,
        Int32(MemoryLayout<z_stream>.size)
    )
    guard status == Z_OK else {
        throw CompressionError.zlibError(code: Int(status))
    }
    defer { deflateEnd(&stream) }
    
    var outputData = Data()
    // Define um buffer para os chunks
    let chunkSize = 4096
    var chunk = [UInt8](repeating: 0, count: chunkSize)
    
    try data.withUnsafeBytes { (srcPointer: UnsafeRawBufferPointer) in
        guard let baseAddress = srcPointer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
            throw CompressionError.compressionFailed
        }
        stream.next_in = UnsafeMutablePointer(mutating: baseAddress)
        stream.avail_in = UInt32(data.count)
        
        repeat {
            let bytesCompressed = try chunk.withUnsafeMutableBytes { buffer -> Int in
                guard let outPtr = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                    throw CompressionError.compressionFailed
                }
                stream.next_out = outPtr
                stream.avail_out = UInt32(chunkSize)
                status = deflate(&stream, Z_FINISH)
                if status != Z_OK && status != Z_STREAM_END {
                    throw CompressionError.zlibError(code: Int(status))
                }
                return chunkSize - Int(stream.avail_out)
            }
            outputData.append(chunk, count: bytesCompressed)
        } while stream.avail_out == 0
    }
    
    guard status == Z_STREAM_END else {
        throw CompressionError.compressionFailed
    }
    return outputData
}

private func gzipDecompress(data: Data) throws -> Data {
    var stream = z_stream()
    var status: Int32 = inflateInit2_(
        &stream,
        15 + 32,    // 15 bits + 32 para detecção automática de header GZIP
        ZLIB_VERSION,
        Int32(MemoryLayout<z_stream>.size)
    )
    guard status == Z_OK else {
        throw CompressionError.zlibError(code: Int(status))
    }
    defer { inflateEnd(&stream) }
    
    var outputData = Data()
    let chunkSize = 4096
    var chunk = [UInt8](repeating: 0, count: chunkSize)
    
    try data.withUnsafeBytes { (srcPointer: UnsafeRawBufferPointer) in
        guard let baseAddress = srcPointer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
            throw CompressionError.decompressionFailed
        }
        stream.next_in = UnsafeMutablePointer(mutating: baseAddress)
        stream.avail_in = UInt32(data.count)
        
        repeat {
            let bytesDecompressed = chunk.withUnsafeMutableBytes { buffer -> Int in
                stream.next_out = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                stream.avail_out = UInt32(chunkSize)
                status = inflate(&stream, Z_SYNC_FLUSH)
                return chunkSize - Int(stream.avail_out)
            }
            if status != Z_OK && status != Z_STREAM_END {
                throw CompressionError.zlibError(code: Int(status))
            }
            chunk.withUnsafeMutableBytes { buffer in
                outputData.append(buffer.bindMemory(to: UInt8.self).baseAddress!, count: bytesDecompressed)
            }
        } while stream.avail_out == 0 && status != Z_STREAM_END
    }
    
    guard status == Z_STREAM_END else {
        throw CompressionError.decompressionFailed
    }
    return outputData
}
