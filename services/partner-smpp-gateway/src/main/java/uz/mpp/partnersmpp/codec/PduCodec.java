package uz.mpp.partnersmpp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * smpp-codec — кодирование/декодирование PDU целиком (header + тело).
 * Предполагает, что декодируемый {@link ByteBuf} содержит ровно один PDU-фрейм
 * (framing по command_length — задача {@code LengthFieldBasedFrameDecoder}
 * в Netty-пайплайне, см. server/SmppFraming.java), не делает
 * partial-read/накопление сам.
 */
public final class PduCodec {

    private PduCodec() {
    }

    /** encode — пишет полный PDU (16-байтовый header с вычисленным command_length + тело) в out. */
    public static void encode(Pdu pdu, ByteBuf out) {
        ByteBuf body = Unpooled.buffer();
        encodeBody(pdu.header().commandId(), pdu.body(), body);

        int commandLength = 16 + body.readableBytes();
        out.writeInt(commandLength);
        out.writeInt(pdu.header().commandId());
        out.writeInt(pdu.header().commandStatus());
        out.writeInt(pdu.header().sequenceNumber());
        out.writeBytes(body);
    }

    /** decode — читает ровно один PDU-фрейм (header уже включён в in). */
    public static Pdu decode(ByteBuf in) {
        int commandLength = in.readInt(); // включает эти же 4 байта — framing уже гарантировал границу
        int commandId = in.readInt();
        int commandStatus = in.readInt();
        int sequenceNumber = in.readInt();

        int bodyLength = commandLength - 16;
        Object body = decodeBody(commandId, in, bodyLength);

        return new Pdu(new PduHeader(commandId, commandStatus, sequenceNumber), body);
    }

    private static void encodeBody(int commandId, Object body, ByteBuf out) {
        switch (commandId) {
            case CommandId.BIND_TRANSCEIVER -> {
                BindTransceiver b = (BindTransceiver) body;
                SmppStrings.writeCString(out, b.systemId());
                SmppStrings.writeCString(out, b.password());
                SmppStrings.writeCString(out, b.systemType());
                out.writeByte(b.interfaceVersion());
                out.writeByte(b.addrTon());
                out.writeByte(b.addrNpi());
                SmppStrings.writeCString(out, b.addressRange());
            }
            case CommandId.BIND_TRANSCEIVER_RESP -> {
                // Реальный, не гипотетический баг, найденный при исправлении
                // HIGH #3 (см. SmppServerHandler.exceptionCaught javadoc):
                // раньше это безусловно кастовало body и звало b.systemId() —
                // SmppServerHandler.respond(..., null) на отклонённый bind
                // (неверный пароль, ESME_RINVPASWD) строит Pdu.headerOnly(...)
                // с body=null НАМЕРЕННО (никакого systemId сообщать нечего),
                // что давало NullPointerException прямо при кодировании
                // самого ответа — партнёр никогда не получал ответ на bind
                // вообще (до этого тихо маскировалось отсутствием
                // exceptionCaught — соединение просто зависало без ответа,
                // не падало видимо; обнаружено тестом
                // bindWithWrongPasswordIsRejectedAndConnectionClosed после
                // добавления exceptionCaught). Пустая C-строка (только NUL,
                // 1 байт) — валидный, симметрично декодируемый тот же
                // decodeBody веткой ниже (не требует отдельного null-case на
                // decode-стороне, тело всё равно 1+ байт).
                BindTransceiverResp b = (BindTransceiverResp) body;
                SmppStrings.writeCString(out, b == null ? "" : b.systemId());
            }
            case CommandId.SUBMIT_SM, CommandId.DELIVER_SM -> {
                ShortMessagePdu b = (ShortMessagePdu) body;
                SmppStrings.writeCString(out, b.serviceType());
                out.writeByte(b.sourceAddrTon());
                out.writeByte(b.sourceAddrNpi());
                SmppStrings.writeCString(out, b.sourceAddr());
                out.writeByte(b.destAddrTon());
                out.writeByte(b.destAddrNpi());
                SmppStrings.writeCString(out, b.destinationAddr());
                out.writeByte(b.esmClass());
                out.writeByte(b.protocolId());
                out.writeByte(b.priorityFlag());
                SmppStrings.writeCString(out, ""); // schedule_delivery_time
                SmppStrings.writeCString(out, ""); // validity_period
                out.writeByte(b.registeredDelivery());
                out.writeByte(b.replaceIfPresentFlag());
                out.writeByte(b.dataCoding());
                out.writeByte(b.smDefaultMsgId());
                byte[] sm = b.shortMessage() == null ? new byte[0] : b.shortMessage();
                out.writeByte(sm.length);
                out.writeBytes(sm);
            }
            case CommandId.SUBMIT_SM_RESP, CommandId.DELIVER_SM_RESP -> {
                // Тот же класс бага, что BIND_TRANSCEIVER_RESP выше —
                // handleSubmitSm отвечает body=null на ESME_RINVBNDSTS/
                // ESME_RTHROTTLED/ESME_RINVMSGLEN (реально отклонённый
                // submit — нет message_id, о котором стоило бы сообщать),
                // раньше это давало NPE прямо при кодировании отказа. Пустая
                // C-строка, не пропуск поля — см. комментарий у
                // BIND_TRANSCEIVER_RESP выше про симметрию с decodeBody.
                ShortMessagePduResp b = (ShortMessagePduResp) body;
                SmppStrings.writeCString(out, b == null ? "" : b.messageId());
            }
            case CommandId.ENQUIRE_LINK, CommandId.ENQUIRE_LINK_RESP,
                 CommandId.UNBIND, CommandId.UNBIND_RESP, CommandId.GENERIC_NACK -> {
                // Пустое тело.
            }
            default -> throw new IllegalArgumentException("encode: неподдерживаемый command_id 0x" + Integer.toHexString(commandId));
        }
    }

    private static Object decodeBody(int commandId, ByteBuf in, int bodyLength) {
        int startReaderIndex = in.readerIndex();
        Object result = switch (commandId) {
            case CommandId.BIND_TRANSCEIVER -> {
                String systemId = SmppStrings.readCString(in);
                String password = SmppStrings.readCString(in);
                String systemType = SmppStrings.readCString(in);
                byte interfaceVersion = in.readByte();
                byte addrTon = in.readByte();
                byte addrNpi = in.readByte();
                String addressRange = SmppStrings.readCString(in);
                yield new BindTransceiver(systemId, password, systemType, interfaceVersion, addrTon, addrNpi, addressRange);
            }
            case CommandId.BIND_TRANSCEIVER_RESP -> new BindTransceiverResp(SmppStrings.readCString(in));
            case CommandId.SUBMIT_SM, CommandId.DELIVER_SM -> {
                String serviceType = SmppStrings.readCString(in);
                byte sourceAddrTon = in.readByte();
                byte sourceAddrNpi = in.readByte();
                String sourceAddr = SmppStrings.readCString(in);
                byte destAddrTon = in.readByte();
                byte destAddrNpi = in.readByte();
                String destinationAddr = SmppStrings.readCString(in);
                byte esmClass = in.readByte();
                byte protocolId = in.readByte();
                byte priorityFlag = in.readByte();
                SmppStrings.readCString(in); // schedule_delivery_time (не используется)
                SmppStrings.readCString(in); // validity_period (не используется)
                byte registeredDelivery = in.readByte();
                byte replaceIfPresentFlag = in.readByte();
                byte dataCoding = in.readByte();
                byte smDefaultMsgId = in.readByte();
                int smLength = in.readByte() & 0xFF;
                byte[] shortMessage = new byte[smLength];
                in.readBytes(shortMessage);
                yield new ShortMessagePdu(serviceType, sourceAddrTon, sourceAddrNpi, sourceAddr,
                    destAddrTon, destAddrNpi, destinationAddr, esmClass, protocolId, priorityFlag,
                    registeredDelivery, replaceIfPresentFlag, dataCoding, smDefaultMsgId, shortMessage);
            }
            case CommandId.SUBMIT_SM_RESP, CommandId.DELIVER_SM_RESP -> new ShortMessagePduResp(SmppStrings.readCString(in));
            case CommandId.ENQUIRE_LINK, CommandId.ENQUIRE_LINK_RESP,
                 CommandId.UNBIND, CommandId.UNBIND_RESP, CommandId.GENERIC_NACK -> null;
            // HIGH находка кодревью #3: раньше IllegalArgumentException здесь
            // распространялось без обработчика (см. MalformedPduException
            // javadoc) — неизвестный command_id от партнёра теперь ловится
            // явно в SmppServerHandler и отвечается GENERIC_NACK, не висит
            // молча.
            default -> throw new MalformedPduException("decode: неизвестный command_id 0x" + Integer.toHexString(commandId));
        };

        int consumed = in.readerIndex() - startReaderIndex;
        int remaining = bodyLength - consumed;
        if (remaining > 0) {
            in.skipBytes(remaining); // необязательные TLV-параметры — пропускаются, не парсятся в этом срезе
        }
        return result;
    }
}