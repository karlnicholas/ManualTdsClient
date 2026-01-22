package com.example;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.RpcPacketBuilder;
import org.tdslib.javatdslib.TdsClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class InsertRpc {
    public static void main(String[] args) throws Exception {
        new InsertRpc().run();
    }

    private void run() throws Exception {
        String hostname = "localhost";
        int port = 1433;
        try ( TdsClient client = new TdsClient(hostname, port) ) {
            client.connect("localhost", "reactnonreact", "reactnonreact", "reactnonreact", "app", "MyServerName", "us_english");
            ByteBuffer rpcBuffer = new RpcPacketBuilder().buildRpcPayload(
        "Michael", "Brown", "mb@m.com", 12);
            client.rpcAsync(rpcBuffer);
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
                System.out.print(PrintColumn.convertRowToString(item));
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

}

