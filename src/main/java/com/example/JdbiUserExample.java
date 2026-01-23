package com.example;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

public class JdbiUserExample {

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
    // JDBI setup – create once (in real apps, use a singleton / injected instance)
    // ────────────────────────────────────────────────
    Jdbi jdbi = Jdbi.create(url, username, password);

    // ────────────────────────────────────────────────
    // SQL with named parameters (:name syntax)
    // ────────────────────────────────────────────────
    String sql = """
            INSERT INTO dbo.users (firstName, lastName, email, postCount)
            VALUES (:firstName, :lastName, :email, :postCount)
            """;

    try (Handle handle = jdbi.open()) {

      handle.createCall("CALL InsertUser(:firstName, :lastName, :email, :postCount)")
          .bind("firstName", firstName)
          .bind("lastName", lastName)
          .bind("email", email)
          .bind("postCount", postCount)
          .invoke();

      System.out.println("Insert successful. Rows affected: ");
      System.out.println("→ Inserted: " + firstName + " " + lastName + " <" + email + "> (postCount = " + postCount + ")");

    } catch (UnableToExecuteStatementException e) {
      System.err.println("SQL Execution Error: " + e.getMessage());
      if (e.getCause() != null) {
        System.err.println("Caused by: " + e.getCause().getMessage());
      }
      e.printStackTrace();
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}