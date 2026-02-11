package com.example;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

public class InsertAllDataTypesJdbc {

  public static void main(String[] args) {

    // ────────────────────────────────────────────────
    // Connection settings
    // ────────────────────────────────────────────────
    String url = "jdbc:sqlserver://localhost:1433;databaseName=reactnonreact;encrypt=false;trustServerCertificate=true";
    String username = "reactnonreact";
    String password = "reactnonreact";


//    Statement statement = client.queryRpc(sql)
//        // --- Exact Numerics ---
//        .bind("test_bit", true)                                      // BIT -> Boolean
//        .bind("test_tinyint", 0xff)                           // TINYINT -> Short (Java byte is signed -128 to 127)
//        .bind("test_smallint", (short) 32000)                        // SMALLINT -> Short
//        .bind("test_int", 2000000000)                                // INT -> Integer
//        .bind("test_bigint", 9000000000000000000L)                   // BIGINT -> Long
//        .bind("test_decimal", new BigDecimal("12345.6789"))          // DECIMAL -> BigDecimal
//        .bind("test_numeric", new BigDecimal("999.99"))              // NUMERIC -> BigDecimal
//        .bind("test_smallmoney", new BigDecimal("214.99"))           // SMALLMONEY -> BigDecimal
//        .bind("test_money", new BigDecimal("922337203685477.58"))    // MONEY -> BigDecimal
//
//        // --- Approximate Numerics ---
//        .bind("test_real", 123.45f)                                  // REAL -> Float
//        .bind("test_float", 123456789.987654321d)                    // FLOAT -> Double
//
//        // --- Date and Time ---
//        .bind("test_date", LocalDate.of(2023, 12, 25))
//        .bind("test_time", LocalTime.parse("14:30:15.1234567"))
//        .bind("test_datetime", LocalDateTime.parse("2023-12-25T14:30:00"))
//        .bind("test_datetime2", LocalDateTime.parse("2023-12-25T14:30:15.1234567"))
//        .bind("test_smalldatetime", LocalDateTime.parse("2023-12-25T14:30:00"))
//        .bind("test_dtoffset", OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30"))
//
//        // --- Character Strings ---
//        .bind("test_char", "FixedChar")
//        .bind("test_varchar", "Variable Length String")
//        .bind("test_varchar_max", "A".repeat(5000))                  // REPLICATE logic moves to Java
//        .bind("test_text", "Legacy Text Data")
//
//        // --- Unicode Strings ---
//        .bind("test_nchar", "FixedUni")
//        .bind("test_nvarchar", "Unicode String")
//        .bind("test_nvarchar_max", "あ".repeat(4000))                 // REPLICATE logic moves to Java
//
//        // --- Binary Strings (Using ByteBuffer or byte[]) ---
//        // Note: 0xDEADBEEF (Hex) needs to be converted to actual Java byte arrays
//        .bind("test_binary", ByteBuffer.wrap(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF}))
//        .bind("test_varbinary", ByteBuffer.wrap(new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE}))
//        .bind("test_varbinary_max", ByteBuffer.wrap(new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC}))
//        .bind("test_image", ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33}))
//
//        // --- Other Data Types ---
//        .bind("test_guid", UUID.randomUUID())                        // NEWID() logic moves to Java
//        .bind("test_xml", "<root><node>Test XML</node></root>");
//              .bind("test_xml", io.r2dbc.mssql.MssqlType.XML.of("<root><node>Test XML</node></root>"));
    // Or simply bind a String for XML if the driver version supports implicit conversion

    String sql = """
    INSERT INTO dbo.AllDataTypes (
        test_bit, test_tinyint, test_smallint, test_int, test_bigint,
        test_decimal, test_numeric, test_smallmoney, test_money,
        test_real, test_float,
        test_date, test_time, test_datetime, test_datetime2, test_smalldatetime, test_dtoffset,
        test_char, test_varchar, test_varchar_max, test_text,
        test_nchar, test_nvarchar, test_nvarchar_max,
        test_binary, test_varbinary, test_varbinary_max, test_image,
        test_guid, test_xml
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?
    )
""";
  try (Connection conn = DriverManager.getConnection(url, username, password);
         PreparedStatement ps = conn.prepareStatement(sql)) {

// --- Exact Numerics ---
      ps.setBoolean(1, true);                                      // test_bit
      ps.setByte(2, (byte) 255);                                 // test_tinyint (Use Short for 0-255 range)
      ps.setShort(3, (short) 32000);                               // test_smallint
      ps.setInt(4, 2000000000);                                    // test_int
      ps.setLong(5, 9000000000000000000L);                         // test_bigint
      ps.setBigDecimal(6, new BigDecimal("12345.6789"));           // test_decimal
      ps.setBigDecimal(7, new BigDecimal("999.99"));               // test_numeric
      ps.setBigDecimal(8, new BigDecimal("214.99"));               // test_smallmoney
      ps.setBigDecimal(9, new BigDecimal("922337203685477.58"));   // test_money

      // --- Approximate Numerics ---
      ps.setFloat(10, 123.45f);                                    // test_real
      ps.setDouble(11, 123456789.987654321d);                      // test_float

      // --- Date and Time (JDBC 4.2 supports java.time directly via setObject) ---
      ps.setObject(12, LocalDate.of(2023, 12, 25));                // test_date
      ps.setObject(13, LocalTime.parse("14:30:15.1234567"));       // test_time
      ps.setObject(14, LocalDateTime.parse("2023-12-25T14:30:00"));// test_datetime
      ps.setObject(15, LocalDateTime.parse("2023-12-25T14:30:15.1234567")); // test_datetime2
      ps.setObject(16, LocalDateTime.parse("2023-12-25T14:30:00"));// test_smalldatetime
      ps.setObject(17, OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30")); // test_dtoffset

      // --- Character Strings ---
      ps.setString(18, "FixedChar");                               // test_char
      ps.setString(19, "Variable Length String");                  // test_varchar
      ps.setString(20, "A".repeat(5000));                          // test_varchar_max
      ps.setString(21, "Legacy Text Data");                        // test_text

      // --- Unicode Strings (setNString is preferred for N-types) ---
      ps.setNString(22, "FixedUni");                               // test_nchar
      ps.setNString(23, "Unicode String");                         // test_nvarchar
      ps.setNString(24, "あ".repeat(4000));                        // test_nvarchar_max

      // --- Binary Strings (JDBC uses byte[], not ByteBuffer) ---
      ps.setBytes(25, new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF}); // test_binary
      ps.setBytes(26, new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE}); // test_varbinary
      ps.setBytes(27, new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC}); // test_varbinary_max
      ps.setBytes(28, new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33}); // test_image

      // --- Other Data Types ---
      ps.setObject(29, UUID.randomUUID());                         // test_guid
      ps.setString(30, "<root><node>Test XML</node></root>");      // test_xml (Drivers usually implicit cast String to XML)


      int rowsAffected = ps.executeUpdate();

      System.out.println("Insert successful. Rows affected: " + rowsAffected);

    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      System.err.println("SQL State: " + e.getSQLState());
      System.err.println("Error Code: " + e.getErrorCode());
      e.printStackTrace();
    }
  }
}