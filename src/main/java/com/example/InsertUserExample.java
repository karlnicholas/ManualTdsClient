package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertUserExample {

  public static void main(String[] args) {

    // ────────────────────────────────────────────────
    // Connection settings
    // ────────────────────────────────────────────────
    String url = "jdbc:sqlserver://localhost:1433;databaseName=reactnonreact;encrypt=false;trustServerCertificate=true";
    String username = "reactnonreact";
    String password = "reactnonreact";

    // Values to insert
    String firstName = "Michael";
    String lastName  = "Brown";
    String email     = "mb@m.com";
    long   postCount = 12;

    // ────────────────────────────────────────────────
    // SQL - we let defaults handle dateJoined, createdAt, updatedAt
    // ────────────────────────────────────────────────
//    String sql = """
//            INSERT INTO dbo.users
//                (firstName, lastName, email, postCount)
//            VALUES
//                (?, ?, ?, ?)
//            """;

    String sql = """
            INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES (?, ?, ?, ?)
            """;
  try (Connection conn = DriverManager.getConnection(url, username, password);
         PreparedStatement ps = conn.prepareStatement(sql)) {

      // Set parameters (1-based indexing)
      ps.setString(1, firstName);
      ps.setString(2, lastName);
      ps.setString(3, email);
      ps.setLong  (4, postCount);

      int rowsAffected = ps.executeUpdate();

      System.out.println("Insert successful. Rows affected: " + rowsAffected);
      System.out.println("→ Inserted: " + firstName + " " + lastName + " <" + email + "> (postCount = " + postCount + ")");

    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      System.err.println("SQL State: " + e.getSQLState());
      System.err.println("Error Code: " + e.getErrorCode());
      e.printStackTrace();
    }
  }
}