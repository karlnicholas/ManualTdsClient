package com.example;

import org.tdslib.javatdslib.QueryResponse;
import org.tdslib.javatdslib.ResultSet;
import org.tdslib.javatdslib.TdsClient;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CSharpTdsClient {
    public static void main(String[] args) throws Exception {
        new CSharpTdsClient().run();
    }

    private void run() throws Exception {
        String hostname = "localhost";
        int port = 1433;
        try ( TdsClient client = new TdsClient(hostname, port) ) {
            client.connect("localhost", "reactnonreact", "reactnonreact", "reactnonreact", "app", "MyServerName", "us_english");
            QueryResponse queryResponse = client.query("select 1; select 2;");
            for(ResultSet resultSet: queryResponse.getResultSets() ) {
                for ( List<byte[]> byteList: resultSet.getRawRows() ) {
                    for( byte[] row: byteList) {
                        System.out.println(new String(row, StandardCharsets.UTF_16LE));
                    }
                }
            }
        }
// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }
}
