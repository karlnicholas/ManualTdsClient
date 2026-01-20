package com.example;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class CSharpTdsClient {
    public static void main(String[] args) throws Exception {
        new CSharpTdsClient().run();
    }

    private void run() throws Exception {
        String hostname = "localhost";
        int port = 1433;
        try ( TdsClient client = new TdsClient(hostname, port) ) {
            client.connect("localhost", "reactnonreact", "reactnonreact", "reactnonreact", "app", "MyServerName", "us_english");
//            queryAsync("SELECT 1; SELECT 2;", client);
//            queryAsync("SELECT @@Version", client);
            String sql = """
                DROP TABLE IF EXISTS dbo.users;
            
                CREATE TABLE dbo.users (
                    id          BIGINT          IDENTITY(1,1)   NOT NULL,
                    firstName   NVARCHAR(100)   NULL,
                    lastName    NVARCHAR(100)   NULL,
                    email       NVARCHAR(254)   NOT NULL,
                    dateJoined  DATE            NULL            DEFAULT CAST(GETDATE() AS DATE),
                    postCount   BIGINT          NULL            DEFAULT 0,
                    createdAt   DATETIME2(3)    NOT NULL        DEFAULT SYSUTCDATETIME(),
                    updatedAt   DATETIME2(3)    NULL,
            
                    CONSTRAINT PK_users         PRIMARY KEY     (id),
                    CONSTRAINT UIX_users_email  UNIQUE          (email)
                );
                """;
              queryAsync(sql, client);
            sql = """
                INSERT INTO dbo.users 
                    (firstName, lastName, email, dateJoined, postCount, createdAt)
                VALUES
                    ('Emma',     'Thompson',  'emma.thompson84@gmail.com',     '2023-05-12',  47,  '2023-05-12T14:30:00Z'),
                    ('Liam',     'Rodriguez', 'liam.r1992@outlook.com',        '2024-01-08', 312,  '2024-01-08T09:15:22Z'),
                    ('Sophia',   'Patel',     'sophia.patel.designs@gmail.com','2022-11-30',  89,  '2022-11-30T18:45:10Z'),
                    ('Noah',     'Kim',       'noah.kim.dev@proton.me',        '2025-02-19', 156,  '2025-02-19T11:20:00Z'),
                    ('Olivia',   'Martinez',  'olivia.martinez.music@yahoo.com','2023-09-04',  23,  '2023-09-04T16:05:33Z'),
                    ('James',    'Wilson',    'j.wilson.photography@gmail.com','2024-06-15', 421,  '2024-06-15T13:40:55Z'),
                    ('Isabella', 'Chen',      'isabella.chen95@icloud.com',    '2021-12-22',  67,  '2021-12-22T20:10:12Z'),
                    ('Ethan',    'Nguyen',    'ethan.nguyen.work@gmail.com',   '2025-03-01', 198,  '2025-03-01T08:55:00Z'),
                    ('Ava',      'Johnson',   'ava.j.creative@outlook.com',    '2023-07-28', 534,  '2023-07-28T22:30:47Z'),
                    ('Benjamin', 'Garcia',    'ben.garcia.tech@protonmail.com','2024-10-17', 105,  '2024-10-17T10:22:19Z');
                """;
            queryAsync(sql, client);
            sql =
                "select * from dbo.users";
            queryAsync(sql, client);

        }
// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }

    private void queryAsync(String sql, TdsClient client) throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        client.queryAsync(sql).subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(RowWithMetadata item) {
                System.out.print(convertRowToString(item));
                System.out.println();
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onComplete() {
                System.out.println("Query complete");
                latch.countDown();
            }
        });
        latch.await();
    }

    /**
     * Very basic string representation of DATETIME2 bytes
     * (real production code should fully parse days + time ticks)
     */
    private String datetime2ToString(byte[] bytes, int scale) {
        if (bytes == null || bytes.length == 0) return "NULL";

        // 1. Time length varies by scale: 0-2 (3 bytes), 3-4 (4 bytes), 5-7 (5 bytes)
        int timeByteLen = (scale <= 2) ? 3 : (scale <= 4) ? 4 : 5;

        // 2. Extract Time Ticks (First part of buffer)
        long ticks = 0;
        for (int i = 0; i < timeByteLen; i++) {
            ticks |= ((long) (bytes[i] & 0xFF)) << (8 * i);
        }

        // 3. Extract Days (Remaining 3 bytes at the end)
        int days = 0;
        for (int i = 0; i < 3; i++) {
            days |= (bytes[timeByteLen + i] & 0xFF) << (8 * i);
        }

        // 4. Conversion to Java Objects
        // Base date for SQL Server is 0001-01-01
        LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);

        // Multiplier to convert ticks to nanoseconds: 10^(9 - scale)
        // Scale 3 = 1,000,000 ns (1ms) per tick. Scale 7 = 100ns per tick.
        long nanoMultiplier = (long) Math.pow(10, 9 - scale);
        LocalTime time = LocalTime.ofNanoOfDay(ticks * nanoMultiplier);

        return date.toString() + " " + time.toString();
    }

    private String convertRowToString(RowWithMetadata rowWithMetadata) {
        StringBuilder sb = new StringBuilder();
        List<ColumnMeta> columns = rowWithMetadata.metadata();
        List<byte[]> rowData = rowWithMetadata.row();  // adjust name if different (e.g. getRowData())

        if (columns.size() != rowData.size()) {
            return "ERROR: Column count mismatch";
        }

        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta col = columns.get(i);
            byte[] raw = rowData.get(i);
            String valueStr;

            if (raw == null) {
                valueStr = "NULL";
            } else {
                valueStr = convertColumnValue(col, raw);
            }

            if (i > 0) {
                sb.append(", ");
            }
            sb.append('"').append(escapeCsv(valueStr)).append('"');
        }

        return sb.toString();
    }

    /**
     * Converts raw bytes of a single column to a human-readable string.
     */
    private String convertColumnValue(ColumnMeta col, byte[] raw) {
        byte type = col.getDataType();

        return switch (type) {
            // Fixed-length integer types
            case (byte) 0x7F -> String.valueOf(bytesToLong(raw));           // BIGINT
            case (byte) 0x38 -> String.valueOf(bytesToInt(raw));            // INT
            case (byte) 0x34 -> String.valueOf(bytesToShort(raw));          // SMALLINT
            case (byte) 0x30 -> String.valueOf(raw[0] & 0xFF);              // TINYINT

            // Floating point
            case (byte) 0x3B -> String.valueOf(bytesToFloat(raw));          // REAL
            case (byte) 0x3E -> String.valueOf(bytesToDouble(raw));         // FLOAT

            // Date types
            case (byte) 0x28 -> dateBytesToString(raw);                     // DATE (3 bytes)

            // DATETIME2(n)
            case (byte) 0x2A -> datetime2ToString(raw, col.getScale());

            // Nullable INT family (already handled as raw bytes of correct length)
            case (byte) 0x26 -> {
                // length already consumed in RowTokenParser → raw is either null or 1/2/4/8 bytes
                if (raw.length == 8) yield String.valueOf(bytesToLong(raw));
                if (raw.length == 4) yield String.valueOf(bytesToInt(raw));
                if (raw.length == 2) yield String.valueOf(bytesToShort(raw));
                if (raw.length == 1) yield String.valueOf(raw[0] & 0xFF);
                yield "ERR: bad intn length " + raw.length;
            }

            // Character types (NVARCHAR most common in your table)
            case (byte) 0xE7, (byte) 0xEF, (byte) 0x27, (byte) 0x2F -> {
                // Assuming UTF-16LE for NVARCHAR/NCHAR
                yield new String(raw, StandardCharsets.UTF_16LE).trim();
            }

            default -> "Unsupported type 0x" + Integer.toHexString(type & 0xFF) +
                " (" + raw.length + " bytes)";
        };
    }

/* ────────────────────────────────────────────────
   Helper conversion methods (little-endian)
──────────────────────────────────────────────── */

    private long bytesToLong(byte[] b) {
        if (b.length != 8) return 0;
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private int bytesToInt(byte[] b) {
        if (b.length != 4) return 0;
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private short bytesToShort(byte[] b) {
        if (b.length != 2) return 0;
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getShort();
    }

    private float bytesToFloat(byte[] b) {
        if (b.length != 4) return 0f;
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    private double bytesToDouble(byte[] b) {
        if (b.length != 8) return 0.0;
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    /**
     * Converts 3-byte DATE to YYYY-MM-DD string
     * (TDS DATE = days since 0001-01-01)
     */
    private String dateBytesToString(byte[] b) {
        if (b.length != 3) return "ERR:DATE";
        int days = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16);
        LocalDate base = LocalDate.of(1, 1, 1);
        return base.plusDays(days).toString();  // requires java.time
    }

    // Simple CSV escape (replace " with "")
    private String escapeCsv(String s) {
        return s.replace("\"", "\"\"");
    }

}

