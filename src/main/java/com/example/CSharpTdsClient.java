package com.example;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;

import java.nio.charset.StandardCharsets;
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
            CountDownLatch latch = new CountDownLatch(1);
            client.queryAsync("SELECT 1; SELECT 2;").subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription subscription;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(RowWithMetadata item) {
                    List<byte[]> row = item.row();
                    for (byte[] column : row) {
                        System.out.print(new String(column, StandardCharsets.UTF_16LE) + " ");
                    }
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
// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }
}


//QueryResponse queryResponse = client.query("select 1; select 2;");
//            for(ResultSet resultSet: queryResponse.getResultSets() ) {
//        for ( List<byte[]> byteList: resultSet.getRawRows() ) {
//        for( byte[] row: byteList) {
//        System.out.println(new String(row, StandardCharsets.UTF_16LE));
//        }
//        }
//        }
