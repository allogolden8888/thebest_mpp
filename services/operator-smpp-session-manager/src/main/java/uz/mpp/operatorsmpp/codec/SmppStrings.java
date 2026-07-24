package uz.mpp.operatorsmpp.codec;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/** SMPP 3.4 C-Octet String helpers — ASCII, null-terminated. */
public final class SmppStrings {
    private SmppStrings() {
    }

    public static void writeCString(ByteBuf out, String value) {
        if (value != null && !value.isEmpty()) {
            out.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
        }
        out.writeByte(0);
    }

    public static String readCString(ByteBuf in) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while ((b = in.readByte()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }
}