package uz.mpp.partnersmpp.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import uz.mpp.partnersmpp.codec.*;
import uz.mpp.partnersmpp.core.IncomingMessageBuilder;
import uz.mpp.partnersmpp.core.SubmitValidator;
import uz.mpp.partnersmpp.core.TokenBucket;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.function.Consumer;

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
    private final Consumer<IncomingMessage> incomingSink;
    private final TokenBucket rateLimiter;
    private final ChannelRegistry channelRegistry;
    private final BindListener bindListener;
    private final UnbindListener unbindListener;

    private boolean bound = false;
    private String partnerId;
    private String applicationId;
    private String systemId;

    public SmppServerHandler(PartnerAuthenticator authenticator, Consumer<IncomingMessage> incomingSink,
                              TokenBucket rateLimiter, ChannelRegistry channelRegistry) {
        this(authenticator, incomingSink, rateLimiter, channelRegistry, null, null);
    }

    public SmppServerHandler(PartnerAuthenticator authenticator, Consumer<IncomingMessage> incomingSink,
                              TokenBucket rateLimiter, ChannelRegistry channelRegistry,
                              BindListener bindListener, UnbindListener unbindListener) {
        this.authenticator = authenticator;
        this.incomingSink = incomingSink;
        this.rateLimiter = rateLimiter;
        this.channelRegistry = channelRegistry;
        this.bindListener = bindListener;
        this.unbindListener = unbindListener;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        Pdu pdu = PduCodec.decode(frame);
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
        incomingSink.accept(incoming);

        String smscMessageId = UUID.randomUUID().toString();
        respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_ROK, seq, new ShortMessagePduResp(smscMessageId));
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