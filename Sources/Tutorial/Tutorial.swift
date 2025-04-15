//
//  Tutorial.swift
//  NyaruDB2
//
//  Created by d.a.albuquerque on 15/04/25.
//

import Foundation
import NyaruDB2

// Define your model type. In this example, we use "User".
struct User: Codable, Equatable {
    let id: Int
    let name: String
    let created_at: String
}

struct Location: Codable, Equatable {
    let id: Int
    let name: String
    let country: String
}


// A simple tutorial function that demonstrates basic CRUD operations
func runTutorial() async {
    do {
        // 1. Initialize the database at the specified path.
        // Data will be persisted in a folder named "NyaruDB_Tutorial".
        let db = try NyaruDB2(
            path: "NyaruDB_Tutorial",
            compressionMethod: .none,     // You can use .gzip, .lzfse, etc.
            fileProtectionType: .none     // Adjust file protection as required
        )
        print("Database initialized successfully.")
        
        // 2. Insert a single document into the "Users" collection.
        // Since no partition key is set globally, documents will be stored in "Users/default.nyaru".
        let user1 = User(id: 1, name: "Alice", created_at: "01/05/2022")
        try await db.insert(user1, into: "Users")
        print("Inserted user1: \(user1)")
        
        // 3. Bulk insert additional documents.
        // Each document will be written to the "Users" collection.
        let bulkUsers = [
            User(id: 2, name: "Bob", created_at: "01/05/2023"),
            User(id: 3, name: "Charlie", created_at: "01/05/2022"),
            User(id: 4, name: "David", created_at: "01/05/2024")
        ]
        try await db.bulkInsert(bulkUsers, into: "Users")
        print("Bulk inserted users: \(bulkUsers)")
        
        
        let bulkLocations = [
            Location(id: 2, name: "Bob", country: "01/05/2023"),
            Location(id: 3, name: "Charlie", country: "01/05/2022"),
            Location(id: 4, name: "David", country: "01/05/2024")
        ]
        try await db.bulkInsert(bulkLocations, into: "Locations")
        print("Bulk inserted users: \(bulkLocations)")

        
        // 4. Query the database.
        // For example, find the user with id = 1.
        var query = try await db.query(from: "Users") as Query<User>
        query.where(\User.id, .equal(1))
        let queryResults = try await query.execute()
        print("Query result for user with id = 1: \(queryResults)")
        
        // 5. Update an existing document.
        // For example, update user1's name from "Alice" to "Alicia".
        let updatedUser1 = User(id: 1, name: "Alicia", created_at: "01/05/2022")
        try await db.update(updatedUser1, in: "Users", matching: { $0.id == 1 }, indexField: "id")
        print("Updated user1 to: \(updatedUser1)")
        
        // 6. Delete a document.
        // For example, delete the user with id = 2.
        try await db.delete(where: { (user: User) -> Bool in user.id == 2 }, from: "Users")
        print("Deleted user with id = 2.")
        
        // 7. Fetch all documents from the collection.
        let allUsers: [User] = try await db.fetch(from: "Users")
        print("All users in 'Users' collection after updates and deletion: \(allUsers)")
        
    } catch {
        print("Error running NyaruDB2 tutorial: \(error)")
    }
}

// Run the tutorial asynchronously.
Task {
    await runTutorial()
    exit(0)
}

// Keep the program running.
dispatchMain()
