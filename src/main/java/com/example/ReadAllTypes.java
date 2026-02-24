package com.example;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ReadAllTypes {

  public static void main(String[] args) {
//    // ────────────────────────────────────────────────
//    // Enable JDBC Driver Trace Logging
//    // ────────────────────────────────────────────────
//    ConsoleHandler handler = new ConsoleHandler();
//    handler.setLevel(Level.FINEST);
//    handler.setFormatter(new SimpleFormatter());
//
//    // Logger for raw TDS packet hex dumps (The bytes sent/received)
//    Logger tdsLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.DATA");
//    tdsLogger.addHandler(handler);
//    tdsLogger.setLevel(Level.FINEST);
//    tdsLogger.setUseParentHandlers(false); // Prevents duplicate logging
//
//    // Optional: General JDBC logger to see the connection sequence
//    Logger jdbcLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Connection");
//    jdbcLogger.addHandler(handler);
//    jdbcLogger.setLevel(Level.FINE);
//    jdbcLogger.setUseParentHandlers(false);

    // ────────────────────────────────────────────────
    // Connection settings
    // ────────────────────────────────────────────────
    String url = "jdbc:sqlserver://localhost:1433;databaseName=reactnonreact;encrypt=false;trustServerCertificate=true";
    String username = "reactnonreact";
    String password = "reactnonreact";

    String sql = "SELECT * FROM dbo.AllDataTypes";

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      int rowCount = 0;

      while (rs.next()) {
        rowCount++;
        System.out.println("--- Reading Row " + rowCount + " ---");

        // --- Exact Numerics ---
        boolean testBit = rs.getBoolean("test_bit");
        short testTinyint = rs.getShort("test_tinyint");
        short testSmallint = rs.getShort("test_smallint");
        int testInt = rs.getInt("test_int");
        long testBigint = rs.getLong("test_bigint");
        BigDecimal testDecimal = rs.getBigDecimal("test_decimal");
        BigDecimal testNumeric = rs.getBigDecimal("test_numeric");
        BigDecimal testSmallmoney = rs.getBigDecimal("test_smallmoney");
        BigDecimal testMoney = rs.getBigDecimal("test_money");

        // --- Approximate Numerics ---
        float testReal = rs.getFloat("test_real");
        double testFloat = rs.getDouble("test_float");

        // --- Date and Time (JDBC 4.2 supports java.time directly via getObject) ---
        LocalDate testDate = rs.getObject("test_date", LocalDate.class);
        LocalTime testTime = rs.getObject("test_time", LocalTime.class);
        LocalDateTime testDatetime = rs.getObject("test_datetime", LocalDateTime.class);
        LocalDateTime testDatetime2 = rs.getObject("test_datetime2", LocalDateTime.class);
        LocalDateTime testSmalldatetime = rs.getObject("test_smalldatetime", LocalDateTime.class);
        OffsetDateTime testDtoffset = rs.getObject("test_dtoffset", OffsetDateTime.class);

        // --- Character Strings ---
        String testChar = rs.getString("test_char");
        String testVarchar = rs.getString("test_varchar");
        String testVarcharMax = rs.getString("test_varchar_max");
        String testText = rs.getString("test_text");

        // --- Unicode Strings (getNString is preferred for N-types) ---
        String testNchar = rs.getNString("test_nchar");
        String testNvarchar = rs.getNString("test_nvarchar");
        String testNvarcharMax = rs.getNString("test_nvarchar_max");

        // --- Binary Strings (JDBC uses byte[]) ---
        byte[] testBinary = rs.getBytes("test_binary");
        byte[] testVarbinary = rs.getBytes("test_varbinary");
        byte[] testVarbinaryMax = rs.getBytes("test_varbinary_max");
        byte[] testImage = rs.getBytes("test_image");

        // --- Other Data Types ---
        String testGuid = rs.getString("test_guid");
        String testXml = rs.getString("test_xml");

        // Print some sample values to the console
        System.out.println("test_varchar: " + testVarchar);
        System.out.println("test_nvarchar: " + testNvarchar);
        System.out.println("test_datetime2: " + testDatetime2);
        System.out.println("test_money: " + testMoney);

        if (testBinary != null) {
          System.out.print("test_binary (hex): ");
          for (byte b : testBinary) {
            System.out.printf("%02X", b);
          }
          System.out.println();
        }
        System.out.println("-------------------------");
      }

      System.out.println("Read successful. Total rows read: " + rowCount);

    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      System.err.println("SQL State: " + e.getSQLState());
      System.err.println("Error Code: " + e.getErrorCode());
      e.printStackTrace();
    }
  }
}