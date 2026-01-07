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
            client.connect("localhost", "reactnonreact", "reactnonreact", "reactnonreact", "app", "MyServerName", "us_english");

        }
// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
    }
}



//import org.tdslib.javatdslib.TdsClient;
//import org.tdslib.javatdslib.io.connection.tcp.TcpServerEndpoint;
//import org.tdslib.javatdslib.messages.Message;
//import org.tdslib.javatdslib.packets.PacketType;
//import org.tdslib.javatdslib.payloads.Payload;
//import org.tdslib.javatdslib.payloads.login7.Login7Options;
//import org.tdslib.javatdslib.payloads.login7.Login7Payload;
//import org.tdslib.javatdslib.payloads.login7.TypeFlags;
//import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
//import org.tdslib.javatdslib.tokens.TokenType;
//
//import java.io.IOException;
//import java.util.concurrent.ExecutionException;
//
//public class CSharpTdsClient {
//    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
//        new CSharpTdsClient().run();
//    }
//
//    private void run() throws IOException, ExecutionException, InterruptedException {
//        String hostname = "192.168.1.121";
//        int port = 1433;
//
//        TdsClient client = new TdsClient(new TcpServerEndpoint(hostname, port));
//
//        // Send PreLogin message
//        Message msg = new Message(PacketType.PRE_LOGIN);
//        msg.setPayload(new PreLoginPayload(false));
//        client.getMessageHandler().sendMessage(msg);
//
//// Receive PreLogin message
//        Message preLoginResponseMessage = client.getMessageHandler().receiveMessage(b->new PreLoginPayload(b));
//
//
//        // Perform TLS handshake
//        System.out.println("client.performTlsHandshake();");
//        client.performTlsHandshake();
//        System.out.println("client.performTlsHandshake();");
//
//        // Prepare Login7 message
//        Login7Payload login7Payload = new Login7Payload(new Login7Options());
//        login7Payload.hostname = "192.168.1.121";
//        login7Payload.serverName = "MyServerName";
//        login7Payload.appName = "MyAppName";
//        login7Payload.language = "us_english";
//        login7Payload.database = "master";
//        login7Payload.username = "reactnonreact";
//        login7Payload.password = "reactnonreact";
//
////        login7Payload..TypeFlags.AccessIntent = TypeFlags.OptionAccessIntent.READ_WRITE;
//
//        Message login7Message = new Message(PacketType.LOGIN7);
//        login7Message.setPayload(login7Payload);
//
//// Send Login7 message
//        client.getMessageHandler().sendMessage(login7Message);
//
////        // Receive Login response tokens
////        System.out.println("receiveTokens");
////        client.getTokenStreamHandler().receiveTokens(tokenEvent -> {
////            System.out.println("tokenEvent.getToken().getType()" + tokenEvent.getToken().getType());
////
////        });
////        client.stopTls();
//        Message lr = client.getMessageHandler().receiveMessage();
//        System.out.println("lr = " + lr);
//
//
//// If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
//    }
//}
