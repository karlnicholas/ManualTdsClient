package com.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Login7ParserCopilot {

    // Utility: extract little-endian unsigned short
    private static int getUShort(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
    }

    // Utility: extract little-endian unsigned int
    private static long getUInt(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
    }

    public static void main(String[] args) {
        // Hex dump as bytes
        String hexdumpStr =
                "10 01 01 0A 00 00 01 00 02 01 00 00 04 00 00 74 " +
                        "00 10 00 00 00 00 00 00 90 6F 00 00 00 00 00 00 " +
                        "81 00 00 08 D4 FE FF FF 09 04 00 00 5E 00 0D 00 " +
                        "78 00 0D 00 92 00 0D 00 AC 00 09 00 BE 00 0C 00 " +
                        "00 00 00 00 D6 00 06 00 E2 00 0A 00 F6 00 06 00 " +
                        "39 94 4E 40 32 20 00 00 00 00 00 00 00 00 00 00 " +
                        "00 00 00 00 00 00 31 00 39 00 32 00 2E 00 31 00 " +
                        "36 00 38 00 2E 00 31 00 2E 00 31 00 32 00 31 00 " +
                        "72 00 65 00 61 00 63 00 74 00 6E 00 6F 00 6E 00 " +
                        "72 00 65 00 61 00 63 00 74 00 82 A5 F3 A5 B3 A5 " +
                        "93 A5 E2 A5 43 A5 53 A5 43 A5 82 A5 F3 A5 B3 A5 " +
                        "93 A5 E2 A5 4D 00 79 00 41 00 70 00 70 00 4E 00 " +
                        "61 00 6D 00 65 00 4D 00 79 00 53 00 65 00 72 00 " +
                        "76 00 65 00 72 00 4E 00 61 00 6D 00 65 00 54 00 " +
                        "64 00 73 00 4C 00 69 00 62 00 75 00 73 00 5F 00 " +
                        "65 00 6E 00 67 00 6C 00 69 00 73 00 68 00 6D 00 " +
                        "61 00 73 00 74 00 65 00 72 00";

        hexdumpStr = hexdumpStr.replaceAll("\\s+", "");
        byte[] data = new byte[hexdumpStr.length() / 2];
        for (int i = 0; i < data.length; ++i)
            data[i] = (byte) Integer.parseInt(hexdumpStr.substring(2 * i, 2 * i + 2), 16);

        int ptr = 0;
        // --- TDS Packet Header (8 bytes) ---
        int packetType = data[ptr++] & 0xFF;
        int status = data[ptr++] & 0xFF;
        int length = getUShort(data, ptr); ptr += 2;
        int spid = getUShort(data, ptr); ptr += 2;
        int packetId = data[ptr++] & 0xFF;
        int window = data[ptr++] & 0xFF;
        System.out.printf("TDS Packet header:\n");
        System.out.printf("  Type: 0x%02X\n", packetType);
        System.out.printf("  Status: 0x%02X\n", status);
        System.out.printf("  Length: %d\n", length);
        System.out.printf("  SPID: %d\n", spid);
        System.out.printf("  PacketID: %d\n", packetId);
        System.out.printf("  Window: %d\n", window);

        // --- LOGIN7 Main Fields ---
        long login7Length = getUInt(data, ptr); ptr += 4;
        long tdsVersion = getUInt(data, ptr); ptr += 4;
        long packetSize = getUInt(data, ptr); ptr += 4;
        long clientProgVer = getUInt(data, ptr); ptr += 4;
        long clientPID = getUInt(data, ptr); ptr += 4;
        long connectionID = getUInt(data, ptr); ptr += 4;
        int optionFlags1 = data[ptr++] & 0xFF;
        int optionFlags2 = data[ptr++] & 0xFF;
        int typeFlags = data[ptr++] & 0xFF;
        int optionFlags3 = data[ptr++] & 0xFF;
        int clientTimeZone = getUInt(data, ptr, 4); ptr += 4;
        int clientLCID = getUInt(data, ptr, 4); ptr += 4;

        System.out.printf("\nLOGIN7 Main Fields:\n");
        System.out.printf("  Length: %d\n", login7Length);
        System.out.printf("  TDSVersion: 0x%08X\n", tdsVersion);
        System.out.printf("  PacketSize: %d\n", packetSize);
        System.out.printf("  ClientProgVer: 0x%08X\n", clientProgVer);
        System.out.printf("  ClientPID: 0x%08X\n", clientPID);
        System.out.printf("  ConnectionID: 0x%08X\n", connectionID);
        System.out.printf("  OptionFlags1: 0x%02X\n", optionFlags1);
        System.out.printf("  OptionFlags2: 0x%02X\n", optionFlags2);
        System.out.printf("  TypeFlags: 0x%02X\n", typeFlags);
        System.out.printf("  OptionFlags3: 0x%02X\n", optionFlags3);
        System.out.printf("  ClientTimeZone: %d\n", clientTimeZone);
        System.out.printf("  ClientLCID: 0x%08X\n", clientLCID);

        // --- Offset/Length fields (9 relevant for variable data) ---
        final int TDS_HEADER_LEN = 8;
        final int LOGIN7_FIXED_LEN = 36;
        final int OFFSETLEN_START = TDS_HEADER_LEN + LOGIN7_FIXED_LEN;
        int[] offsets = new int[9];
        int[] lengths = new int[9];
        String[] names = {
                "HostName", "UserName", "Password", "AppName", "ServerName",
                "Unused", "CltIntName", "Language", "Database"
        };
        int ofsPtr = OFFSETLEN_START;
        for (int i = 0; i < 9; ++i) {
            offsets[i] = getUShort(data, ofsPtr); ofsPtr += 2;
            lengths[i] = getUShort(data, ofsPtr); ofsPtr += 2;
        }
        // Now parse ClientID (6 bytes), then SSPI/AtchDBFile/ChangePassword/SSPILong
        int clientIdPos = ofsPtr;
        byte[] clientId = Arrays.copyOfRange(data, clientIdPos, clientIdPos + 6);
        ofsPtr += 6;

        int ibSSPI = getUShort(data, ofsPtr); ofsPtr += 2;
        int cbSSPI = getUShort(data, ofsPtr); ofsPtr += 2;
        int ibAtchDBFile = getUShort(data, ofsPtr); ofsPtr += 2;
        int cchAtchDBFile = getUShort(data, ofsPtr); ofsPtr += 2;
        int ibChangePassword = getUShort(data, ofsPtr); ofsPtr += 2;
        int cchChangePassword = getUShort(data, ofsPtr); ofsPtr += 2;
        long cbSSPILong = getUInt(data, ofsPtr); ofsPtr += 4;

        // --- Print OffsetLength Table
        System.out.println("\nOffsetLength Table:");
        for (int i = 0; i < 9; ++i) {
            System.out.printf("  %-12s: Offset=%3d  Length=%d\n", names[i], offsets[i], lengths[i]);
        }
        System.out.printf("  ClientID: %s\n", bytesToHex(clientId));
        System.out.printf("  ibSSPI=%d cbSSPI=%d\n", ibSSPI, cbSSPI);
        System.out.printf("  ibAtchDBFile=%d cchAtchDBFile=%d\n", ibAtchDBFile, cchAtchDBFile);
        System.out.printf("  ibChangePassword=%d cchChangePassword=%d\n", ibChangePassword, cchChangePassword);
        System.out.printf("  cbSSPILong=%d\n", cbSSPILong);

        // --- Extract Variable Data ---
        System.out.println("\nVariable-length Data Values:");
        for (int i = 0; i < 9; i++) {
            if ("Unused".equals(names[i])) continue;
            // Corrected: offset is from start of LOGIN7, i.e. add TDS_HEADER_LEN (8)
            printUnicodeField(names[i], data, TDS_HEADER_LEN + offsets[i], lengths[i]);
        }
    }

    private static int getUInt(byte[] buf, int ofs, int len) {
        // For 4-bytes (DWORD) only
        return ByteBuffer.wrap(buf, ofs, len).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static void printUnicodeField(String name, byte[] data, int offset, int length) {
        if (length == 0) {
            System.out.printf("%-12s: [Empty]%n", name);
            return;
        }
        int byteLen = length * 2;
        if (offset == 0) {
            System.out.printf("%-12s: [Invalid Offset]%n", name);
            return;
        }
        if (offset + byteLen > data.length) {
            System.out.printf("%-12s: [Out of Bounds]%n", name);
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            int hi = data[offset + i * 2 + 1] & 0xFF;
            int lo = data[offset + i * 2] & 0xFF;
            char c = (char)((hi << 8) | lo);
            sb.append(c);
        }
        System.out.printf("%-12s: %s%n", name, sb.toString());
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
            sb.append(String.format("%02X", b));
        return sb.toString();
    }
}