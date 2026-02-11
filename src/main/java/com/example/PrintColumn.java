package com.example;

public class PrintColumn {

//  /**
//   * Converts raw bytes of a single column to a human-readable string.
//   */
//  private static String convertColumnValue(ColumnMetadata col, Object raw) {
////    byte type = ;
//
//    return switch (col.getType()) {
//      // Fixed-length integer types
//      case (byte) 0x7F -> String.valueOf(bytesToLong(raw));           // BIGINT
//      case (byte) 0x38 -> String.valueOf(bytesToInt(raw));            // INT
//      case (byte) 0x34 -> String.valueOf(bytesToShort(raw));          // SMALLINT
//      case (byte) 0x30 -> String.valueOf(raw[0] & 0xFF);              // TINYINT
//
//      // Floating point
//      case (byte) 0x3B -> String.valueOf(bytesToFloat(raw));          // REAL
//      case (byte) 0x3E -> String.valueOf(bytesToDouble(raw));         // FLOAT
//
//      // Date types
//      case (byte) 0x28 -> dateBytesToString(raw);                     // DATE (3 bytes)
//
//      // DATETIME2(n)
//      case (byte) 0x2A -> datetime2ToString(raw, col.getScale());
//
//      // Nullable INT family (already handled as raw bytes of correct length)
//      case (byte) 0x26 -> {
//        // length already consumed in RowTokenParser → raw is either null or 1/2/4/8 bytes
//        if (raw.length == 8) yield String.valueOf(bytesToLong(raw));
//        if (raw.length == 4) yield String.valueOf(bytesToInt(raw));
//        if (raw.length == 2) yield String.valueOf(bytesToShort(raw));
//        if (raw.length == 1) yield String.valueOf(raw[0] & 0xFF);
//        yield "ERR: bad intn length " + raw.length;
//      }
//
//      // Character types (NVARCHAR most common in your table)
//      case (byte) 0xE7, (byte) 0xEF, (byte) 0x27, (byte) 0x2F -> {
//        // Assuming UTF-16LE for NVARCHAR/NCHAR
//        yield new String(raw, StandardCharsets.UTF_16LE).trim();
//      }
//
//      default -> "Unsupported type 0x" + Integer.toHexString(type & 0xFF) +
//          " (" + raw.length + " bytes)";
//    };
//  }
//
//  /**
//   * Very basic string representation of DATETIME2 bytes
//   * (real production code should fully parse days + time ticks)
//   */
//  private static String datetime2ToString(byte[] bytes, int scale) {
//    if (bytes == null || bytes.length == 0) return "NULL";
//
//    // 1. Time length varies by scale: 0-2 (3 bytes), 3-4 (4 bytes), 5-7 (5 bytes)
//    int timeByteLen = (scale <= 2) ? 3 : (scale <= 4) ? 4 : 5;
//
//    // 2. Extract Time Ticks (First part of buffer)
//    long ticks = 0;
//    for (int i = 0; i < timeByteLen; i++) {
//      ticks |= ((long) (bytes[i] & 0xFF)) << (8 * i);
//    }
//
//    // 3. Extract Days (Remaining 3 bytes at the end)
//    int days = 0;
//    for (int i = 0; i < 3; i++) {
//      days |= (bytes[timeByteLen + i] & 0xFF) << (8 * i);
//    }
//
//    // 4. Conversion to Java Objects
//    // Base date for SQL Server is 0001-01-01
//    LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);
//
//    // Multiplier to convert ticks to nanoseconds: 10^(9 - scale)
//    // Scale 3 = 1,000,000 ns (1ms) per tick. Scale 7 = 100ns per tick.
//    long nanoMultiplier = (long) Math.pow(10, 9 - scale);
//    LocalTime time = LocalTime.ofNanoOfDay(ticks * nanoMultiplier);
//
//    return date.toString() + " " + time.toString();
//  }
//
//  public static String convertRowToString(Result result) {
//    StringBuilder sb = new StringBuilder();
//    result.map((row, rowMetadata) -> {
////      List<ColumnMeta> columns = result..metadata();
////      List<byte[]> rowData = result.row();  // adjust name if different (e.g. getRowData())
//
//      for (int i = 0; i < row.getMetadata().getColumnMetadatas().size(); i++) {
//        ColumnMetadata col = row.getMetadata().getColumnMetadatas().get(i);
//        Object raw = row.get(i);
//        String valueStr;
//
//        if (raw == null) {
//          valueStr = "NULL";
//        } else {
//          valueStr = convertColumnValue(col, raw);
//        }
//
//        if (i > 0) {
//          sb.append(", ");
//        }
//        sb.append('"').append(escapeCsv(valueStr)).append('"');
//      }
//
//    });
//    return sb.toString();
//  }
//
///* ────────────────────────────────────────────────
//   Helper conversion methods (little-endian)
//──────────────────────────────────────────────── */
//
//  private static long bytesToLong(byte[] b) {
//    if (b.length != 8) return 0;
//    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong();
//  }
//
//  private static int bytesToInt(byte[] b) {
//    if (b.length != 4) return 0;
//    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
//  }
//
//  private static short bytesToShort(byte[] b) {
//    if (b.length != 2) return 0;
//    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getShort();
//  }
//
//  private static float bytesToFloat(byte[] b) {
//    if (b.length != 4) return 0f;
//    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getFloat();
//  }
//
//  private static double bytesToDouble(byte[] b) {
//    if (b.length != 8) return 0.0;
//    return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getDouble();
//  }
//
//  /**
//   * Converts 3-byte DATE to YYYY-MM-DD string
//   * (TDS DATE = days since 0001-01-01)
//   */
//  private static String dateBytesToString(byte[] b) {
//    if (b.length != 3) return "ERR:DATE";
//    int days = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16);
//    LocalDate base = LocalDate.of(1, 1, 1);
//    return base.plusDays(days).toString();  // requires java.time
//  }
//
//  // Simple CSV escape (replace " with "")
//  private static String escapeCsv(String s) {
//    return s.replace("\"", "\"\"");
//  }
//
}
