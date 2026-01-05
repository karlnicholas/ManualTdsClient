package com.example;

import com.microsoft.data.tools.tdslib.io.connection.tcp.TcpConnection;
import com.microsoft.data.tools.tdslib.io.connection.tcp.TcpConnectionOptions;
import com.microsoft.data.tools.tdslib.io.connection.tcp.TcpServerEndpoint;
// UPDATED IMPORT: Login7Payload is now in the .login7 package
import com.microsoft.data.tools.tdslib.payloads.login7.Login7Payload;
import com.microsoft.data.tools.tdslib.payloads.prelogin.EncryptionType;
import com.microsoft.data.tools.tdslib.payloads.prelogin.PreLoginPayload;

import java.nio.ByteBuffer;
import java.util.HexFormat;

public class ManualTdsClient {

    public static void main(String[] args) {
        try {
            System.out.println("--- Starting JavaTdsLib Full Login Test ---");

            // 1. Configure Endpoint
            var endpoint = new TcpServerEndpoint("192.168.1.121", 1433);

            // 2. Configure Options
            var options = new TcpConnectionOptions();
            options.setReceiveTimeout(5000);
            options.setPacketSize(4096);

            System.out.println("Connecting to " + endpoint.hostname() + "...");

            // 3. Connect
            try (var connection = new TcpConnection(options, endpoint)) {
                System.out.println("TCP Connected!");

                // ==========================================
                // STEP A: PRE-LOGIN (Plaintext)
                // ==========================================
                System.out.println("\n>>> Sending PRE-LOGIN...");

                var preLogin = new PreLoginPayload();
                preLogin.setEncryption(EncryptionType.OFF); // 0x00
                preLogin.setVersion(16, 0, 0, 0);
                preLogin.setThreadId(Thread.currentThread().getId());
                preLogin.setInstanceName("");
                preLogin.setMarsEnabled(false);

                // Generate & Wrap
                ByteBuffer preLoginData = preLogin.generate();
                int plLen = 8 + preLoginData.remaining();
                ByteBuffer plPacket = ByteBuffer.allocate(plLen);

                plPacket.put((byte) 0x12);            // Type: Pre-Login
                plPacket.put((byte) 0x01);            // Status: EOM
                plPacket.putShort((short) plLen);     // Length
                plPacket.putShort((short) 0);         // SPID
                plPacket.put((byte) 0);
                plPacket.put((byte) 0);
                plPacket.put(preLoginData);
                plPacket.flip();

                connection.sendData(plPacket);
                System.out.println("Pre-Login Sent.");

                // Read Response
                var plResponse = connection.receiveData();
                System.out.println("<<< Received PRE-LOGIN RESPONSE (" + plResponse.remaining() + " bytes)");

                // ==========================================
                // STEP B: TLS HANDSHAKE
                // ==========================================
                System.out.println("\n>>> Starting TLS Handshake...");

                // This triggers the doHandshake() loop in TcpConnection
                // It will pump packets using PreLoginTlsChannel (Type 0x12)
                connection.startTLS();

                System.out.println("TLS Handshake Complete! Connection is now encrypted.");

                // ==========================================
                // STEP C: LOGIN7 (Encrypted)
                // ==========================================
                System.out.println("\n>>> Sending LOGIN7 Credentials...");

                var login7 = new Login7Payload();
                login7.setHostName("localhost");
                login7.setAppName("JavaTdsLib");
                login7.setServerName("192.168.1.121");
                login7.setLibraryName("jtds");
                login7.setLanguage("us_english");
                login7.setDatabase("master");

                // CREDENTIALS
                login7.setUserName("reactnonreact");
                login7.setPassword("reactnonreact");

                login7.setPacketSize(4096);

                // Generate Payload
                ByteBuffer loginData = login7.generate();

                // Wrap in TDS Header (Type 0x10 = Login)
                // NOTE: Even though we are encrypted, we still write a TDS Header.
                // The TcpConnection will encrypt the WHOLE thing (Header + Data).
                int l7Len = 8 + loginData.remaining();
                ByteBuffer l7Packet = ByteBuffer.allocate(l7Len);

                l7Packet.put((byte) 0x10);            // Type: Login7
                l7Packet.put((byte) 0x01);            // Status: EOM
                l7Packet.putShort((short) l7Len);     // Length
                l7Packet.putShort((short) 0);         // SPID
                l7Packet.put((byte) 0);
                l7Packet.put((byte) 0);
                l7Packet.put(loginData);
                l7Packet.flip();

                // Send (TcpConnection will encrypt this automatically now)
                connection.sendData(l7Packet);
                System.out.println("Login7 Packet Sent (" + l7Len + " bytes raw).");

                // ==========================================
                // STEP D: READ LOGIN RESPONSE
                // ==========================================
                System.out.println("\nWaiting for Login Response...");

                // This will read encrypted data, decrypt it, and return plaintext TDS
                var loginResponse = connection.receiveData();

                System.out.println("<<< Received LOGIN RESPONSE (" + loginResponse.remaining() + " bytes)");

                byte[] respBytes = new byte[loginResponse.remaining()];
                loginResponse.get(respBytes);
                System.out.println(HexFormat.of().formatHex(respBytes));
            }
        } catch (Exception e) {
            System.err.println("Test Failed!");
            e.printStackTrace();
        }
    }
}