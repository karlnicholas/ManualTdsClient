package com.example;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class InsertUserCallableExample {

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
    // SQL – using JDBC escape syntax for stored procedure call
    // ────────────────────────────────────────────────
    String sql = "{call dbo.InsertUser(?, ?, ?, ?)}";

    try (Connection conn = DriverManager.getConnection(url, username, password);
         CallableStatement cstmt = conn.prepareCall(sql)) {

      // Set parameters – positional (1-based)
      // Order must match the order of parameters in the stored procedure
      cstmt.setString(1, firstName);    // corresponds to @firstName
      cstmt.setString(2, lastName);     // corresponds to @lastName
      cstmt.setString(3, email);        // corresponds to @email
      cstmt.setLong  (4, postCount);    // corresponds to @postCount

      int rowsAffected = cstmt.executeUpdate();

      System.out.println("Call successful. Rows affected: " + rowsAffected);
      System.out.println("→ Inserted: " + firstName + " " + lastName + " <" + email + "> (postCount = " + postCount + ")");

    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      System.err.println("SQL State: " + e.getSQLState());
      System.err.println("Error Code: " + e.getErrorCode());
      e.printStackTrace();
    }
  }
}