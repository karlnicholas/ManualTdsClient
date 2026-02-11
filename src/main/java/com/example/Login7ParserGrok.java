package com.example;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Login7ParserGrok {
    public static void main(String[] args) {
        String hexDump =
                "10 01 01 0A 00 00 01 00 02 01 00 00 04 00 00 74 " +
                        "00 10 00 00 00 00 00 00 88 74 00 00 00 00 00 00 " +
                        "81 00 00 08 D4 FE FF FF 09 04 00 00 5E 00 0D 00 " +
                        "78 00 0D 00 92 00 0D 00 AC 00 09 00 BE 00 0C 00 " +
                        "00 00 00 00 D6 00 06 00 E2 00 0A 00 F6 00 06 00 " +
                        "8A 82 5B 04 84 0E 00 00 00 00 00 00 00 00 00 00 " +
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
        // Convert hex string to byte array
        String[] hexBytes = hexDump.split(" ");
        byte[] bytes = new byte[hexBytes.length];
        for (int i = 0; i < hexBytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hexBytes[i], 16);
        }

        // Verify packet header
        if (bytes[0] != 0x10) {
            System.out.println("Not a LOGIN7 packet");
            return;
        }

        int packetLength = (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
        if (packetLength != bytes.length) {
            System.out.println("Invalid packet length");
            return;
        }

        // Start parsing LOGIN7 from position 8
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.position(8);

        // Fixed fields
        int length = bb.getInt();
        System.out.println("Length: " + length);

        int tdsVersion = bb.getInt();
        System.out.println("TDSVersion: 0x" + Integer.toHexString(tdsVersion).toUpperCase());

        int packetSize = bb.getInt();
        System.out.println("PacketSize: " + packetSize);

        int clientProgVer = bb.getInt();
        System.out.println("ClientProgVer: " + clientProgVer);

        int clientPID = bb.getInt();
        System.out.println("ClientPID: " + clientPID);

        int connectionID = bb.getInt();
        System.out.println("ConnectionID: " + connectionID);

        byte optionFlags1 = bb.get();
        System.out.println("OptionFlags1: 0x" + String.format("%02X", optionFlags1));
        printOptionFlags1(optionFlags1);

        byte optionFlags2 = bb.get();
        System.out.println("OptionFlags2: 0x" + String.format("%02X", optionFlags2));
        printOptionFlags2(optionFlags2);

        byte typeFlags = bb.get();
        System.out.println("TypeFlags: 0x" + String.format("%02X", typeFlags));
        printTypeFlags(typeFlags);

        byte optionFlags3 = bb.get();
        System.out.println("OptionFlags3: 0x" + String.format("%02X", optionFlags3));
        printOptionFlags3(optionFlags3);

        int clientTimeZone = bb.getInt();
        System.out.println("ClientTimeZone: " + clientTimeZone);

        int clientLCID = bb.getInt();
        System.out.println("ClientLCID: 0x" + Integer.toHexString(clientLCID).toUpperCase());

        // OffsetLength
        int startOfVariable = bb.position(); // Position after fixed part

        int ibHostName = bb.getShort() & 0xFFFF;
        int cchHostName = bb.getShort() & 0xFFFF;
        System.out.println("ibHostName: " + ibHostName + ", cchHostName: " + cchHostName);

        int ibUserName = bb.getShort() & 0xFFFF;
        int cchUserName = bb.getShort() & 0xFFFF;
        System.out.println("ibUserName: " + ibUserName + ", cchUserName: " + cchUserName);

        int ibPassword = bb.getShort() & 0xFFFF;
        int cchPassword = bb.getShort() & 0xFFFF;
        System.out.println("ibPassword: " + ibPassword + ", cchPassword: " + cchPassword);

        int ibAppName = bb.getShort() & 0xFFFF;
        int cchAppName = bb.getShort() & 0xFFFF;
        System.out.println("ibAppName: " + ibAppName + ", cchAppName: " + cchAppName);

        int ibServerName = bb.getShort() & 0xFFFF;
        int cchServerName = bb.getShort() & 0xFFFF;
        System.out.println("ibServerName: " + ibServerName + ", cchServerName: " + cchServerName);

        int ibExtension = bb.getShort() & 0xFFFF; // or unused
        int cbExtension = bb.getShort() & 0xFFFF;
        System.out.println("ibExtension: " + ibExtension + ", cbExtension: " + cbExtension);

        int ibCltIntName = bb.getShort() & 0xFFFF;
        int cchCltIntName = bb.getShort() & 0xFFFF;
        System.out.println("ibCltIntName: " + ibCltIntName + ", cchCltIntName: " + cchCltIntName);

        int ibLanguage = bb.getShort() & 0xFFFF;
        int cchLanguage = bb.getShort() & 0xFFFF;
        System.out.println("ibLanguage: " + ibLanguage + ", cchLanguage: " + cchLanguage);

        int ibDatabase = bb.getShort() & 0xFFFF;
        int cchDatabase = bb.getShort() & 0xFFFF;
        System.out.println("ibDatabase: " + ibDatabase + ", cchDatabase: " + cchDatabase);

        byte[] clientID = new byte[6];
        bb.get(clientID);
        System.out.print("ClientID: ");
        for (byte b : clientID) {
            System.out.print(String.format("%02X ", b));
        }
        System.out.println();

        int ibSSPI = bb.getShort() & 0xFFFF;
        int cbSSPI = bb.getShort() & 0xFFFF;
        System.out.println("ibSSPI: " + ibSSPI + ", cbSSPI: " + cbSSPI);

        int ibAtchDBFile = bb.getShort() & 0xFFFF;
        int cchAtchDBFile = bb.getShort() & 0xFFFF;
        System.out.println("ibAtchDBFile: " + ibAtchDBFile + ", cchAtchDBFile: " + cchAtchDBFile);

        int ibChangePassword = bb.getShort() & 0xFFFF;
        int cchChangePassword = bb.getShort() & 0xFFFF;
        System.out.println("ibChangePassword: " + ibChangePassword + ", cchChangePassword: " + cchChangePassword);

        int cbSSPILong = bb.getInt();
        System.out.println("cbSSPILong: " + cbSSPILong);

        // Now extract strings
        System.out.println("\nVariable Data:");

        if (cchHostName > 0) {
            bb.position(8 + ibHostName);
            System.out.println("HostName: " + getString(bb, cchHostName, false));
        }

        if (cchUserName > 0) {
            bb.position(8 + ibUserName);
            System.out.println("UserName: " + getString(bb, cchUserName, false));
        }

        if (cchPassword > 0) {
            bb.position(8 + ibPassword);
            System.out.println("Password (deobfuscated): " + getString(bb, cchPassword, true));
        } else {
            System.out.println("Password: (empty)");
        }

        if (cchAppName > 0) {
            bb.position(8 + ibAppName);
            System.out.println("AppName: " + getString(bb, cchAppName, false));
        }

        if (cchServerName > 0) {
            bb.position(8 + ibServerName);
            System.out.println("ServerName: " + getString(bb, cchServerName, false));
        }

        if (cchCltIntName > 0) {
            bb.position(8 + ibCltIntName);
            System.out.println("CltIntName: " + getString(bb, cchCltIntName, false));
        }

        if (cchLanguage > 0) {
            bb.position(8 + ibLanguage);
            System.out.println("Language: " + getString(bb, cchLanguage, false));
        }

        if (cchDatabase > 0) {
            bb.position(8 + ibDatabase);
            System.out.println("Database: " + getString(bb, cchDatabase, false));
        }

        if (cbSSPI > 0 || cbSSPILong > 0) {
            // Handle SSPI data if present
            System.out.println("SSPI data present, but not parsing.");
        }

        if (cchAtchDBFile > 0) {
            bb.position(8 + ibAtchDBFile);
            System.out.println("AtchDBFile: " + getString(bb, cchAtchDBFile, false));
        }

        if (cchChangePassword > 0) {
            bb.position(8 + ibChangePassword);
            System.out.println("ChangePassword (deobfuscated): " + getString(bb, cchChangePassword, true));
        }

        // If fExtension == 1, parse FeatureExt, but in this case it's 0
    }

//    private static String getString(ByteBuffer bb, int charLength, boolean isPassword) {
//        byte[] data = new byte[charLength * 2];
//        bb.get(data);
//        if (isPassword) {
//            for (int i = 0; i < data.length; i++) {
//                byte b = data[i];
//                int swapped = ((b & 0xF0) >>> 4) | ((b & 0x0F) << 4);
//                data[i] = (byte) (swapped ^ 0xA5);
//            }
//        }
//        return new String(data, StandardCharsets.UTF_16LE);
//    }
//
private static String getString(ByteBuffer bb, int charLength, boolean isPassword) {
    byte[] data = new byte[charLength * 2];
    bb.get(data);

    if (isPassword) {
        for (int i = 0; i < data.length; i++) {
            int b = data[i] & 0xFF;

            // Step 1: Undo XOR first
            int temp = b ^ 0xA5;

            // Step 2: Rotate right by 4 bits (undo the client's left rotate)
            int original = ((temp >>> 4) & 0x0F) | ((temp << 4) & 0xF0);

            data[i] = (byte) original;
        }
    }

    return new String(data, StandardCharsets.UTF_16LE);
}

private static void printOptionFlags1(byte flags) {
        System.out.println("  fByteOrder: " + ((flags & 0x01) == 0 ? "ORDER_X86" : "ORDER_68000"));
        System.out.println("  fChar: " + ((flags & 0x02) == 0 ? "CHARSET_ASCII" : "CHARSET_EBCDIC"));
        int fFloat = (flags & 0x0C) >>> 2;
        String floatType = fFloat == 0 ? "FLOAT_IEEE_754" : fFloat == 1 ? "FLOAT_VAX" : "ND5000";
        System.out.println("  fFloat: " + floatType);
        System.out.println("  fDumpLoad: " + ((flags & 0x10) == 0 ? "DUMPLOAD_ON" : "DUMPLOAD_OFF"));
        System.out.println("  fUseDB: " + ((flags & 0x20) == 0 ? "USE_DB_OFF" : "USE_DB_ON"));
        System.out.println("  fDatabase: " + ((flags & 0x40) == 0 ? "INIT_DB_WARN" : "INIT_DB_FATAL"));
        System.out.println("  fSetLang: " + ((flags & 0x80) == 0 ? "SET_LANG_OFF" : "SET_LANG_ON"));
    }

    private static void printOptionFlags2(byte flags) {
        System.out.println("  fLanguage: " + ((flags & 0x01) == 0 ? "INIT_LANG_WARN" : "INIT_LANG_FATAL"));
        System.out.println("  fODBC: " + ((flags & 0x02) == 0 ? "ODBC_OFF" : "ODBC_ON"));
        // fTranBoundary removed in 7.2, reserved
        // fCacheConnect removed in 7.2, reserved
        int fUserType = (flags & 0x1C) >>> 2;
        String userType = fUserType == 0 ? "USER_NORMAL" : fUserType == 1 ? "USER_SERVER" : fUserType == 2 ? "USER_REMUSER" : "USER_SQLREPL";
        System.out.println("  fUserType: " + userType);
        System.out.println("  fIntSecurity: " + ((flags & 0x80) == 0 ? "INTEGRATED_SECURITY_OFF" : "INTEGRATED_SECURITY_ON"));
    }

    private static void printTypeFlags(byte flags) {
        int fSQLType = flags & 0x0F;
        System.out.println("  fSQLType: " + (fSQLType == 0 ? "SQL_DFLT" : "SQL_TSQL"));
        System.out.println("  fOLEDB: " + ((flags & 0x10) == 0 ? "OLEDB_OFF" : "OLEDB_ON"));
        System.out.println("  fReadOnlyIntent: " + ((flags & 0x20) == 0 ? "0" : "1"));
        // reserved bits
    }

    private static void printOptionFlags3(byte flags) {
        System.out.println("  fChangePassword: " + ((flags & 0x01) == 0 ? "0" : "1"));
        System.out.println("  fSendYukonBinaryXML: " + ((flags & 0x02) == 0 ? "0" : "1"));
        System.out.println("  fUserInstance: " + ((flags & 0x04) == 0 ? "0" : "1"));
        System.out.println("  fUnknownCollationHandling: " + ((flags & 0x08) == 0 ? "0" : "1"));
        System.out.println("  fExtension: " + ((flags & 0x10) == 0 ? "0" : "1"));
        // reserved bits
    }
}