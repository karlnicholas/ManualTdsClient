package com.example;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;
import java.util.UUID;

public record AllDataTypesRecord(
        Integer id,

        // Exact Numerics
        Boolean testBit,
        Byte testTinyInt,     // CHANGED: Short -> Byte
        Short testSmallInt,
        Integer testInt,
        Long testBigInt,
        BigDecimal testDecimal,
        BigDecimal testNumeric,
        BigDecimal testSmallMoney,
        BigDecimal testMoney,

        // Approximate Numerics
        Float testReal,
        Double testFloat,

        // Date and Time
        LocalDate testDate,
        LocalTime testTime,
        LocalDateTime testDatetime,
        LocalDateTime testDatetime2,
        LocalDateTime testSmallDatetime,
        OffsetDateTime testDtOffset,

        // Character Strings (Non-Unicode)
        String testChar,
        String testVarchar,
        String testVarcharMax,
        String testText,

        // Unicode Strings
        String testNChar,
        String testNVarchar,
        String testNVarcharMax,

        // Binary Strings
        byte[] testBinary,
        byte[] testVarbinary,
        byte[] testVarbinaryMax,
        byte[] testImage,

        // Other
        UUID testGuid,
        String testXml
) {
  @Override
  public String toString() {
    return "VALUES (\n" +
            "    " + fmt(testBit) + ",       -- test_bit\n" +
            "    " + fmt(testTinyInt) + ",     -- test_tinyint\n" + // Calls fmt(Byte)
            "    " + fmt(testSmallInt) + ",   -- test_smallint\n" +
            "    " + fmt(testInt) + ",        -- test_int\n" +
            "    " + fmt(testBigInt) + ",     -- test_bigint\n" +
            "    " + fmt(testDecimal) + ",    -- test_decimal\n" +
            "    " + fmt(testNumeric) + ",    -- test_numeric\n" +
            "    " + fmt(testSmallMoney) + ", -- test_smallmoney\n" +
            "    " + fmt(testMoney) + ",      -- test_money\n" +
            "    " + fmt(testReal) + ",       -- test_real\n" +
            "    " + fmt(testFloat) + ",      -- test_float\n" +
            "    " + fmtSql(testDate) + ",   -- test_date\n" +
            "    " + fmtSql(testTime) + ",   -- test_time\n" +
            "    " + fmtSql(testDatetime) + ", -- test_datetime\n" +
            "    " + fmtSql(testDatetime2) + ", -- test_datetime2\n" +
            "    " + fmtSql(testSmallDatetime) + ", -- test_smalldatetime\n" +
            "    " + fmtSql(testDtOffset) + ", -- test_dtoffset\n" +
            "    " + fmtSql(testChar, false) + ", -- test_char\n" +
            "    " + fmtSql(testVarchar, false) + ", -- test_varchar\n" +
            "    " + fmtSql(testVarcharMax, false) + ", -- test_varchar_max\n" +
            "    " + fmtSql(testText, false) + ", -- test_text\n" +
            "    " + fmtSql(testNChar, true) + ", -- test_nchar\n" +
            "    " + fmtSql(testNVarchar, true) + ", -- test_nvarchar\n" +
            "    " + fmtSql(testNVarcharMax, true) + ", -- test_nvarchar_max\n" +
            "    " + fmtHex(testBinary) + ",  -- test_binary\n" +
            "    " + fmtHex(testVarbinary) + ", -- test_varbinary\n" +
            "    " + fmtHex(testVarbinaryMax) + ", -- test_varbinary_max\n" +
            "    " + fmtHex(testImage) + ",   -- test_image\n" +
            "    " + fmtSql(testGuid) + ",   -- test_guid\n" +
            "    " + fmtSql(testXml, false) + " -- test_xml\n" +
            ");";
  }

  // --- Helpers ---

  // NEW: Specific handler for Byte to display as unsigned (0-255)
  private String fmt(Byte b) {
    return b == null ? "NULL" : String.valueOf(Byte.toUnsignedInt(b));
  }

  // Handles Short, Integer, Long, BigDecimal, Float, Double
  private String fmt(Number n) {
    return n == null ? "NULL" : String.valueOf(n);
  }

  private String fmt(Boolean b) {
    return b == null ? "NULL" : (b ? "1" : "0");
  }

  private String fmtSql(String s, boolean unicode) {
    if (s == null) return "NULL";
    String prefix = unicode ? "N" : "";
    String val = s.replace("'", "''");
    if (val.length() > 50 && val.matches("A+")) val = "REPLICATE('A', " + val.length() + ")";
    else if (val.length() > 50 && val.matches("あ+")) val = "REPLICATE(N'あ', " + val.length() + ")";
    else val = "'" + val + "'";
    return prefix + val;
  }

  private String fmtSql(Temporal t) {
    if (t == null) return "NULL";
    return "'" + t.toString().replace("T", " ") + "'";
  }

  private String fmtSql(UUID u) {
    return u == null ? "NULL" : "'" + u.toString() + "'";
  }

  private String fmtHex(byte[] bytes) {
    if (bytes == null) return "NULL";
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}