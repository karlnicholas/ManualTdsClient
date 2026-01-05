package com.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Login7ParserGemini {

    private static final String HEX_DUMP =
            "10 01 01 0A 00 00 01 00 02 01 00 00 04 00 00 74 " +
                    "00 10 00 00 00 00 00 00 D8 52 00 00 00 00 00 00 " +
                    "81 00 00 08 D4 FE FF FF 09 04 00 00 5E 00 0D 00 " +
                    "78 00 0D 00 92 00 0D 00 AC 00 09 00 BE 00 0C 00 " +
                    "00 00 00 00 D6 00 06 00 E2 00 0A 00 F6 00 06 00 " +
                    "FF C0 41 F8 44 43 00 00 00 00 00 00 00 00 00 00 " +
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

    public static void main(String[] args) {
        byte[] data = hexStringToByteArray(HEX_DUMP.replace(" ", ""));
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // --- 1. TDS HEADER (Big Endian) ---
        buffer.order(ByteOrder.BIG_ENDIAN);
        System.out.println("=== TDS HEADER (8 bytes) ===");
        System.out.printf("Type:       0x%02X%n", buffer.get());
        System.out.printf("Status:     0x%02X%n", buffer.get());
        System.out.printf("Length:     %d bytes%n", Short.toUnsignedInt(buffer.getShort()));
        System.out.printf("SPID:       %d%n", Short.toUnsignedInt(buffer.getShort()));
        System.out.printf("Packet ID:  %d%n", buffer.get());
        System.out.printf("Window:     %d%n", buffer.get());
        System.out.println();

        // --- 2. LOGIN7 BODY (Little Endian) ---
        int login7Start = 8;
        buffer.position(login7Start);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        System.out.println("=== LOGIN7 FIXED HEADER ===");
        System.out.printf("Total Length:    %d%n", buffer.getInt());
        System.out.printf("TDS Version:     0x%08X%n", buffer.getInt());
        System.out.printf("Packet Size:     %d%n", buffer.getInt());
        System.out.printf("Client Prog Ver: 0x%08X%n", buffer.getInt());
        System.out.printf("Client PID:      %d%n", buffer.getInt());
        System.out.printf("Connection ID:   %d%n", buffer.getInt());

        byte flags1 = buffer.get();
        byte flags2 = buffer.get();
        byte typeFlags = buffer.get();
        byte flags3 = buffer.get();

        System.out.printf("Option Flags 1:  0x%02X%n", flags1);
        System.out.printf("Option Flags 2:  0x%02X%n", flags2);
        System.out.printf("Type Flags:      0x%02X%n", typeFlags);
        System.out.printf("Option Flags 3:  0x%02X%n", flags3);
        System.out.printf("Client TimeZone: %d%n", buffer.getInt());
        System.out.printf("Client LCID:     0x%04X%n", buffer.getInt());
        System.out.println();

        // --- 3. VARIABLE DATA SECTION ---
        System.out.println("=== VARIABLE DATA SECTION ===");
        System.out.println("Field            : [Offset][Len]   : Value");
        System.out.println("-----------------------------------------------------");

        // Helper variables to store offset (ib) and length (cch/cb)
        int ibHost, cchHost, ibUser, cchUser, ibPwd, cchPwd, ibApp, cchApp;
        int ibServer, cchServer, ibExt, cbExt, ibLib, cchLib, ibLang, cchLang;
        int ibDb, cchDb, ibSSPI, cbSSPI, ibAtch, cchAtch, ibChPwd, cchChPwd;

        ibHost = Short.toUnsignedInt(buffer.getShort()); cchHost = Short.toUnsignedInt(buffer.getShort());
        ibUser = Short.toUnsignedInt(buffer.getShort()); cchUser = Short.toUnsignedInt(buffer.getShort());
        ibPwd  = Short.toUnsignedInt(buffer.getShort()); cchPwd  = Short.toUnsignedInt(buffer.getShort());
        ibApp  = Short.toUnsignedInt(buffer.getShort()); cchApp  = Short.toUnsignedInt(buffer.getShort());
        ibServer=Short.toUnsignedInt(buffer.getShort()); cchServer=Short.toUnsignedInt(buffer.getShort());
        ibExt  = Short.toUnsignedInt(buffer.getShort()); cbExt   = Short.toUnsignedInt(buffer.getShort());
        ibLib  = Short.toUnsignedInt(buffer.getShort()); cchLib  = Short.toUnsignedInt(buffer.getShort());
        ibLang = Short.toUnsignedInt(buffer.getShort()); cchLang = Short.toUnsignedInt(buffer.getShort());
        ibDb   = Short.toUnsignedInt(buffer.getShort()); cchDb   = Short.toUnsignedInt(buffer.getShort());

        // ClientID (Fixed 6 bytes)
        byte[] macBytes = new byte[6];
        buffer.get(macBytes);

        ibSSPI = Short.toUnsignedInt(buffer.getShort()); cbSSPI  = Short.toUnsignedInt(buffer.getShort());
        ibAtch = Short.toUnsignedInt(buffer.getShort()); cchAtch = Short.toUnsignedInt(buffer.getShort());
        ibChPwd= Short.toUnsignedInt(buffer.getShort()); cchChPwd= Short.toUnsignedInt(buffer.getShort());

        // cbSSPILong (4 bytes)
        long cbSSPILong = Integer.toUnsignedLong(buffer.getInt());

        // Print Fields
        printField(data, login7Start, "Host Name", ibHost, cchHost);
        printField(data, login7Start, "User Name", ibUser, cchUser);
        printField(data, login7Start, "Password", ibPwd, cchPwd);
        printField(data, login7Start, "App Name", ibApp, cchApp);
        printField(data, login7Start, "Server Name", ibServer, cchServer);
        printField(data, login7Start, "Extension", ibExt, cbExt);
        printField(data, login7Start, "Library Name", ibLib, cchLib);
        printField(data, login7Start, "Language", ibLang, cchLang);
        printField(data, login7Start, "Database", ibDb, cchDb);

        // Print ClientID separately
        System.out.printf("%-16s : [Fixed][6]      : %02X-%02X-%02X-%02X-%02X-%02X%n",
                "Client ID", macBytes[0], macBytes[1], macBytes[2], macBytes[3], macBytes[4], macBytes[5]);

        printField(data, login7Start, "SSPI", ibSSPI, cbSSPI);
        printField(data, login7Start, "Attach DB", ibAtch, cchAtch);
        printField(data, login7Start, "Change Pass", ibChPwd, cchChPwd);
    }

    private static void printField(byte[] data, int baseAddr, String label, int offset, int length) {
        String meta = String.format("[%d][%d]", offset, length);

        if (length == 0) {
            System.out.printf("%-16s : %-15s : [Empty]%n", label, meta);
            return;
        }

        int absStart = baseAddr + offset;

        // Determine byte length (cch fields are x2, cb fields are x1)
        boolean isBinary = label.equals("Extension") || label.equals("SSPI");
        int byteLen = isBinary ? length : length * 2;

        if (absStart + byteLen > data.length) {
            System.out.printf("%-16s : %-15s : [Error: Out of Bounds]%n", label, meta);
            return;
        }

        String value;
        if (isBinary) {
            value = "[Binary Data]";
        } else if (label.equals("Password") || label.equals("Change Pass")) {
            // DECODE PASSWORD
            byte[] rawBytes = Arrays.copyOfRange(data, absStart, absStart + byteLen);
            value = decodeTdsPassword(rawBytes);
        } else {
            // Strings are UCS-2LE (UTF-16LE)
            value = new String(data, absStart, byteLen, StandardCharsets.UTF_16LE);
        }

        System.out.printf("%-16s : %-15s : %s%n", label, meta, value);
    }

    /**
     * Decodes the SQL Server TDS password obfuscation.
     * Logic: XOR 0xA5 then swap high/low nibbles.
     */
    private static String decodeTdsPassword(byte[] scrambledBytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < scrambledBytes.length; i += 2) {
            // Process low byte
            int low = (scrambledBytes[i] & 0xFF) ^ 0xA5;
            low = ((low & 0x0F) << 4) | ((low & 0xF0) >> 4);

            // Process high byte (usually 0 for ASCII, but can be part of unicode char)
            int high = 0;
            if (i + 1 < scrambledBytes.length) {
                high = (scrambledBytes[i+1] & 0xFF) ^ 0xA5;
                high = ((high & 0x0F) << 4) | ((high & 0xF0) >> 4);
            }

            // Combine into char
            char c = (char) (low | (high << 8));
            sb.append(c);
        }
        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}