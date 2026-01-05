package com.example;

import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.io.connection.tcp.TcpServerEndpoint;

public class CSharpTdsClient {
    public static void main(String[] args) throws Exception {
        new CSharpTdsClient().run();
    }

    private void run() throws Exception {
        String hostname = "localhost";
        int port = 1433;
        try ( TdsClient client = new TdsClient(hostname, port) ) {
            client.connect("reactnonreact", "reactnonreact", "reactnonreact", "app");

        }
// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }
}
