import Compression
import Foundation

/// Uma árvore B autobalanceada para indexar dados.
/// Essa implementação mapeia chaves do tipo Key para arrays de Data.
/// Agora implementada como um actor para garantir segurança concorrente.
public actor BTreeIndex<Key: Comparable & Codable> {

    // MARK: - Definição Interna do Nó
    private final class Node: Codable, @unchecked Sendable {
        var keys: [Key] = []
        var values: [[Data]] = []
        var children: [Node] = []
        var isLeaf: Bool

        init(isLeaf: Bool) {
            self.isLeaf = isLeaf
        }

        private enum CodingKeys: String, CodingKey {
            case keys, values, children, isLeaf
        }
    }

    private let t: Int
    private var root: Node

    private let serializationQueue = DispatchQueue(
        label: "com.nyarudb2.btree.serialization"
    )

    /// Inicializa a B-Tree com o grau mínimo desejado (padrão: 2).
    public init(minimumDegree: Int = 2) {
        precondition(minimumDegree >= 2, "O grau mínimo deve ser pelo menos 2.")
        self.t = minimumDegree
        self.root = Node(isLeaf: true)
    }

    // MARK: - Operações Básicas

    public func search(key: Key) -> [Data]? {
        return search(in: root, key: key)
    }

    private func search(in node: Node, key: Key) -> [Data]? {
        var i = 0
        while i < node.keys.count && key > node.keys[i] {
            i += 1
        }
        if i < node.keys.count && key == node.keys[i] {
            return node.values[i]
        }
        if node.isLeaf {
            return nil
        } else {
            return search(in: node.children[i], key: key)
        }
    }

    public func loadPersistedIndex(from data: Data) async throws {
        let decoder = JSONDecoder()
        let decodedRoot = try decoder.decode(Node.self, from: data)
        self.root = decodedRoot
    }

    public func loadPersistedIndex(from url: URL) async throws {
        let data = try Data(contentsOf: url)
        try await loadPersistedIndex(from: data)
    }

    public func insert(key: Key, data: Data) {
        if let existing = search(key: key), !existing.isEmpty {
            insertDataIfExists(in: root, key: key, data: data)
            return
        }
        if root.keys.count == (2 * t - 1) {
            let s = Node(isLeaf: false)
            s.children.append(root)
            splitChild(parent: s, index: 0, child: root)
            root = s
            insertNonFull(node: root, key: key, data: data)
        } else {
            insertNonFull(node: root, key: key, data: data)
        }
    }

    private func insertDataIfExists(in node: Node, key: Key, data: Data) {
        var i = 0
        while i < node.keys.count && key > node.keys[i] {
            i += 1
        }
        if i < node.keys.count && key == node.keys[i] {
            node.values[i].append(data)
            return
        }
        if node.isLeaf {
            node.keys.insert(key, at: i)
            node.values.insert([data], at: i)
        } else {
            insertDataIfExists(in: node.children[i], key: key, data: data)
        }
    }

    private func insertNonFull(node: Node, key: Key, data: Data) {
        var i = node.keys.count - 1
        if node.isLeaf {
            while i >= 0 && key < node.keys[i] {
                i -= 1
            }
            let insertIndex = i + 1
            node.keys.insert(key, at: insertIndex)
            node.values.insert([data], at: insertIndex)
        } else {
            while i >= 0 && key < node.keys[i] {
                i -= 1
            }
            i += 1
            if node.children[i].keys.count == (2 * t - 1) {
                splitChild(parent: node, index: i, child: node.children[i])
                if key > node.keys[i] {
                    i += 1
                }
            }
            insertNonFull(node: node.children[i], key: key, data: data)
        }
    }

    private func splitChild(parent: Node, index: Int, child: Node) {
        let newNode = Node(isLeaf: child.isLeaf)
        newNode.keys = Array(child.keys[t...])
        newNode.values = Array(child.values[t...])
        if !child.isLeaf {
            newNode.children = Array(child.children[t...])
            child.children.removeSubrange(t..<child.children.count)
        }
        child.keys.removeSubrange(t..<child.keys.count)
        child.values.removeSubrange(t..<child.values.count)

        parent.children.insert(newNode, at: index + 1)
        // Seleciona a chave mediana para subir para o pai
        let medianKey = child.keys[t - 1]
        let medianValue = child.values[t - 1]
        parent.keys.insert(medianKey, at: index)
        parent.values.insert(medianValue, at: index)

        child.keys.remove(at: t - 1)
        child.values.remove(at: t - 1)
    }

    // MARK: - Paginação / Lazy Loading de Dados

    /// Executa uma travessia in-order e retorna todos os Data armazenados de forma ordenada.
    public func inOrder() -> [Data] {
        var result: [Data] = []
        inOrderTraversal(node: root, result: &result)
        return result
    }

    private func inOrderTraversal(node: Node, result: inout [Data]) {
        if node.isLeaf {
            for valueGroup in node.values {
                result.append(contentsOf: valueGroup)
            }
        } else {
            for i in 0..<node.keys.count {
                inOrderTraversal(node: node.children[i], result: &result)
                result.append(contentsOf: node.values[i])
            }
            if let lastChild = node.children.last {
                inOrderTraversal(node: lastChild, result: &result)
            }
        }
    }

    public func persist(to url: URL) async throws {
        let currentRoot = self.root
        try await withCheckedThrowingContinuation { continuation in
            serializationQueue.async {
                do {
                    // Encode the tree (here we encode the root which is recursive)
                    let encoder = JSONEncoder()
                    let data = try encoder.encode(currentRoot)

                    // Optionally, compress the data before writing.
                    let compressedData = try? compressData(data, method: .gzip)

                    // Write the data (compressed or not) to disk.
                    try (compressedData ?? data).write(
                        to: url,
                        options: .atomic
                    )

                    // Resume after successful persist.
                    continuation.resume()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Retorna uma "página" dos dados – ou seja, uma parte dos Data em ordem – com base em um offset e limite.
    public func page(offset: Int, limit: Int) -> [Data] {
        let allData = inOrder()
        guard offset < allData.count else { return [] }
        let end = min(allData.count, offset + limit)
        return Array(allData[offset..<end])
    }

    public func getTotalCount() async -> Int {
        return inOrder().count
    }
}
