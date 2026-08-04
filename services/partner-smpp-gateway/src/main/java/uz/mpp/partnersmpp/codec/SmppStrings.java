package uz.mpp.partnersmpp.codec;

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

    /**
     * HIGH находка кодревью (CODE_REVIEW.md, "partner-smpp-gateway" #3):
     * раньше цикл читал {@code in.readByte()} без границы — {@code system_id}
     * без NUL-терминатора читал через границы поля до полного исчерпания
     * 64 KiB фрейма, бросая {@link IndexOutOfBoundsException} без единого
     * обработчика в пайплайне. Теперь ограничено {@code in.readableBytes()}
     * (уже гарантированно == остаток ОДНОГО PDU-фрейма, framing решён
     * frame decoder'ом раньше — см. {@link PduCodec}) и бросает
     * {@link MalformedPduException}, которую {@code SmppServerHandler}
     * ловит явно и отвечает GENERIC_NACK, вместо неявного краха на
     * over-read.
     */
    public static String readCString(ByteBuf in) {
        StringBuilder sb = new StringBuilder();
        while (in.isReadable()) {
            byte b = in.readByte();
            if (b == 0) {
                return sb.toString();
            }
            sb.append((char) b);
        }
        throw new MalformedPduException("C-string без NUL-терминатора: фрейм исчерпан после " + sb.length() + " байт");
    }
}