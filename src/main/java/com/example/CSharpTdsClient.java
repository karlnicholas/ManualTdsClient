package com.example;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.tdslib.javatdslib.TdsClient;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;


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
            asyncQuery(sql, client);
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
            asyncQuery(sql, client);
            sql = "select * from dbo.users";
            asyncQuery(sql, client);
            //

//            sql = "INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES (@p1, @p2, @p3, @p4)";
//            PreparedRpcQuery prp = client.queryRpc(sql)
//                .bind("@p1", "Michael")
//                .bind("@p2", "Thomas")
//                .bind("@p3", "mt@mt.com")
//                .bind("@p4", 12L);
//
//            rpcAsyncUpdate(prp, client);

//            sql = """
//                SELECT @retval = COUNT(*) FROM dbo.users WHERE postCount > @p1
//                """;
            sql = """
                SELECT * FROM dbo.users WHERE postCount > @p1
                """;
            Statement statement = client.queryRpc(sql)
                .bind("@p1", 100L);

            rpcAsyncQuery(sql, client);

//            CountDownLatch latch = new CountDownLatch(1);
//            // 1. The Source: A publisher of R2DBC Results
//            statement.execute()            // If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }

  }
  BiFunction<Row, RowMetadata, DbRecord> mapper =  (row, meta) -> new DbRecord(
          row.get(0, Long.class),
          row.get(1, String.class),
          row.get(2, String.class),
          row.get(3, String.class),
          row.get(4, LocalDate.class),
          row.get(5, Long.class),
          row.get(6, LocalDateTime.class),
          row.get(7, LocalDateTime.class)
  );

  private void asyncQuery(String sql, TdsClient client) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    MappingProducer.from(client.queryAsync(sql).execute())
            .flatMap(result -> result.map(mapper))
            .subscribe(
                    System.out::println,
                    throwable -> {
                      System.out.println("Error: " + throwable.getMessage());
                      latch.countDown();
                    },
                    latch::countDown
            );
    latch.await();
  }

  private void rpcAsyncQuery(String sql, TdsClient client) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    MappingProducer.from(client.queryAsync(sql).execute())
            .flatMap(result -> result.map(mapper))
            .subscribe(
                    System.out::println,
                    throwable -> {
                      System.out.println("Error: " + throwable.getMessage());
                      latch.countDown();
                    },
                    latch::countDown
            );
    latch.await();
  }

}

