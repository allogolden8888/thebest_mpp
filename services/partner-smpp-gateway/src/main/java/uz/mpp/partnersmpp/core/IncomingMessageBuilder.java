package uz.mpp.partnersmpp.core;

import com.google.protobuf.Timestamp;
import uz.mpp.partnersmpp.codec.ShortMessagePdu;
import uz.mpp.platformcontracts.common.v1.Channel;
import uz.mpp.platformcontracts.common.v1.SmsPayload;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.time.Instant;
import java.util.UUID;

/**
 * build_incoming_message (service_internal_methods.md §1.2): ValidatedSubmit
 * + session -> IncomingMessage (platform-contracts/events/message_events.proto).
 *
 * <p>{@code segment_count} сознательно не заполняется здесь — комментарий
 * SmsPayload.segment_count в common/types.proto: "Посчитано один раз
 * Pipeline Engine при кэшировании контекста" — Partner SMPP Gateway не
 * дублирует этот расчёт.
 */
public final class IncomingMessageBuilder {

    private IncomingMessageBuilder() {
    }

    private static String dataCodingToEncoding(byte dataCoding) {
        return switch (dataCoding) {
            case 0 -> "GSM7";
            case 8 -> "UCS2";
            default -> "GSM7";
        };
    }

    public static IncomingMessage build(ShortMessagePdu pdu, String partnerId, String applicationId, Instant messageTtl) {
        String messageId = UUID.randomUUID().toString();
        String traceId = UUID.randomUUID().toString();

        SmsPayload sms = SmsPayload.newBuilder()
            .setMsisdn(pdu.destinationAddr())
            .setSender(pdu.sourceAddr())
            .setBody(new String(pdu.shortMessage() == null ? new byte[0] : pdu.shortMessage()))
            .setEncoding(dataCodingToEncoding(pdu.dataCoding()))
            .build();

        Instant now = Instant.now();
        return IncomingMessage.newBuilder()
            .setMessageId(messageId)
            .setTraceId(traceId)
            .setChannel(Channel.CHANNEL_SMS)
            .setPartnerId(partnerId)
            .setApplicationId(applicationId)
            .setSms(sms)
            .setReceivedAt(toTimestamp(now))
            .setMessageTtl(toTimestamp(messageTtl))
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}