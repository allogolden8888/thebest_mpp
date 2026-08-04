package uz.mpp.partnersmpp.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import uz.mpp.partnersmpp.codec.*;
import uz.mpp.partnersmpp.core.IncomingMessageBuilder;
import uz.mpp.partnersmpp.core.SubmitValidator;
import uz.mpp.partnersmpp.core.TokenBucket;
import uz.mpp.partnersmpp.kafkaio.IncomingPublishFunction;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * handle_bind / validate_submit_pdu / check_rate_limit / build_incoming_message
 * / publish_incoming / send_submit_sm_resp / handle_enquire_link / handle_unbind
 * (service_internal_methods.md §1.2) — по одному инстансу на TCP-соединение
 * (Netty channel), состояние сессии живёт в полях этого handler'а.
 */
public final class SmppServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    /** onBind/onUnbind — опциональные хуки для внешней (Redis) регистрации сессии, см. Main.java. */
    public interface BindListener {
        void onBind(String partnerId, String systemId, long sessionEpoch);
    }

    public interface UnbindListener {
        void onUnbind(String partnerId, String systemId);
    }

    private final PartnerAuthenticator authenticator;
    private final IncomingPublishFunction incomingSink;
    private final TokenBucket rateLimiter;
    private final ChannelRegistry channelRegistry;
    private final BindListener bindListener;
    private final UnbindListener unbindListener;

    private boolean bound = false;
    private String partnerId;
    private String applicationId;
    private String systemId;

    public SmppServerHandler(PartnerAuthenticator authenticator, IncomingPublishFunction incomingSink,
                              TokenBucket rateLimiter, ChannelRegistry channelRegistry) {
        this(authenticator, incomingSink, rateLimiter, channelRegistry, null, null);
    }

    public SmppServerHandler(PartnerAuthenticator authenticator, IncomingPublishFunction incomingSink,
                              TokenBucket rateLimiter, ChannelRegistry channelRegistry,
                              BindListener bindListener, UnbindListener unbindListener) {
        this.authenticator = authenticator;
        this.incomingSink = incomingSink;
        this.rateLimiter = rateLimiter;
        this.channelRegistry = channelRegistry;
        this.bindListener = bindListener;
        this.unbindListener = unbindListener;
    }

    /**
     * HIGH находка кодревью #3 — раньше {@code PduCodec.decode(frame)} мог
     * бросить исключение (неизвестный command_id, C-string без
     * NUL-терминатора), которое распространялось необработанным в Netty
     * default tail: молча логировалось и глушилось, партнёр зависал без
     * ответа, GENERIC_NACK-ветка ниже (строка с {@code default ->}) была
     * мёртвым кодом, потому что до неё никогда не доходило. Теперь decode
     * обёрнут — malformed PDU отвечается GENERIC_NACK с лучшей попыткой
     * извлечь sequence_number из заголовка (валиден независимо от тела).
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        frame.markReaderIndex();
        Pdu pdu;
        try {
            pdu = PduCodec.decode(frame);
        } catch (Exception e) {
            int fallbackSeq = tryExtractSequenceNumber(frame);
            if (fallbackSeq != -1) {
                respond(ctx, CommandId.GENERIC_NACK, CommandStatus.ESME_RINVCMDID, fallbackSeq, null);
            } else {
                ctx.close();
            }
            return;
        }
        int seq = pdu.header().sequenceNumber();

        switch (pdu.header().commandId()) {
            case CommandId.BIND_TRANSCEIVER -> handleBind(ctx, (BindTransceiver) pdu.body(), seq);
            case CommandId.SUBMIT_SM -> handleSubmitSm(ctx, (ShortMessagePdu) pdu.body(), seq);
            case CommandId.ENQUIRE_LINK -> respond(ctx, CommandId.ENQUIRE_LINK_RESP, CommandStatus.ESME_ROK, seq, null);
            case CommandId.UNBIND -> handleUnbind(ctx, seq);
            case CommandId.DELIVER_SM_RESP -> {
                // Ответ партнёра на deliver_sm, отправленный gRPC-путём
                // (handle_deliver_sm_command). Корреляция по sequence_number
                // с ожидающим gRPC-вызовом не реализована в этом срезе —
                // см. README "Что НЕ реализовано" (push сейчас fire-and-forget).
            }
            default -> respond(ctx, CommandId.GENERIC_NACK, CommandStatus.ESME_RINVCMDID, seq, null);
        }
    }

    /** Заголовок (16 байт: command_length/command_id/command_status/sequence_number) — всегда фиксированного формата, читаем его напрямую, независимо от того, распарсилось ли тело. */
    private int tryExtractSequenceNumber(ByteBuf frame) {
        frame.resetReaderIndex();
        if (frame.readableBytes() < 16) {
            return -1;
        }
        frame.readInt(); // command_length
        frame.readInt(); // command_id
        frame.readInt(); // command_status
        return frame.readInt(); // sequence_number
    }

    /**
     * HIGH находка кодревью #3 (malformed PDU не отвечен) — обрыв
     * соединения/необработанное исключение из любого другого источника
     * внутри пайплайна (не только decode) не должно тихо повиснуть в Netty
     * default tail без хотя бы диагностики; соединение закрывается, дальше
     * решает reconnect-логика клиента (партнёр переподключится).
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    private void handleBind(ChannelHandlerContext ctx, BindTransceiver body, int seq) {
        PartnerAuthenticator.AuthResult auth = authenticator.authenticate(body.systemId(), body.password());
        if (!auth.allowed()) {
            respond(ctx, CommandId.BIND_TRANSCEIVER_RESP, CommandStatus.ESME_RINVPASWD, seq, null);
            ctx.close();
            return;
        }
        this.bound = true;
        this.partnerId = auth.partnerId();
        this.applicationId = auth.applicationId();
        this.systemId = body.systemId();
        if (channelRegistry != null) {
            long epoch = channelRegistry.register(partnerId, systemId, ctx.channel());
            if (bindListener != null) {
                bindListener.onBind(partnerId, systemId, epoch);
            }
        }
        respond(ctx, CommandId.BIND_TRANSCEIVER_RESP, CommandStatus.ESME_ROK, seq, new BindTransceiverResp(body.systemId()));
    }

    private void handleSubmitSm(ChannelHandlerContext ctx, ShortMessagePdu body, int seq) {
        if (!bound) {
            respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_RINVBNDSTS, seq, null);
            return;
        }
        if (!rateLimiter.tryAcquire(System.currentTimeMillis())) {
            respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_RTHROTTLED, seq, null);
            return;
        }

        SubmitValidator.ValidationResult validation = SubmitValidator.validate(body);
        if (!validation.valid()) {
            respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_RINVMSGLEN, seq, null);
            return;
        }

        IncomingMessage incoming = IncomingMessageBuilder.build(body, partnerId, applicationId, Instant.now().plus(1, ChronoUnit.DAYS));

        // HIGH находка кодревью #2: раньше submit_sm_resp=ESME_ROK
        // отправлялся немедленно (fire-and-forget publish, Future
        // отбрасывался) — транзиентная недоступность Kafka теряла
        // сообщение НАВСЕГДА, партнёру говорилось "принято". Теперь ответ
        // отправляется только после реального ack от Kafka (или ошибки).
        // Callback может сработать на I/O-потоке продюсера, не на Netty
        // event loop — запись в ctx безопасна из любого потока (Netty сам
        // передиспетчеризует на event loop канала).
        String smscMessageId = UUID.randomUUID().toString();
        incomingSink.publish(incoming, (ignored, error) -> {
            if (error != null) {
                respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_RSYSERR, seq, null);
            } else {
                respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_ROK, seq, new ShortMessagePduResp(smscMessageId));
            }
        });
    }

    private void handleUnbind(ChannelHandlerContext ctx, int seq) {
        respond(ctx, CommandId.UNBIND_RESP, CommandStatus.ESME_ROK, seq, null);
        bound = false;
        deregister(ctx);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        deregister(ctx);
        super.channelInactive(ctx);
    }

    private void deregister(ChannelHandlerContext ctx) {
        if (channelRegistry != null && partnerId != null) {
            channelRegistry.unregisterIfSameChannel(partnerId, systemId, ctx.channel());
            if (unbindListener != null) {
                unbindListener.onUnbind(partnerId, systemId);
            }
            partnerId = null; // idempotency guard — evita двойной unregister (unbind + channelInactive)
        }
    }

    private void respond(ChannelHandlerContext ctx, int commandId, int status, int seq, Object body) {
        Pdu resp = body == null ? Pdu.headerOnly(commandId, status, seq) : Pdu.withBody(commandId, status, seq, body);
        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(resp, out);
        ctx.writeAndFlush(out);
    }

    public boolean isBound() {
        return bound;
    }
}