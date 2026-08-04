package uz.mpp.operatorsmpp.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import uz.mpp.operatorsmpp.codec.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Клиентская сторона SMPP-сессии к оператору (bind_operator, handle_submit_sm_resp,
 * handle_raw_dlr, service_internal_methods.md §1.3). Корреляция ответов по
 * sequence_number — простой "smpp-window": {@link #pendingResponses}.
 */
public final class OperatorSmppClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final Map<Integer, CompletableFuture<Pdu>> pendingResponses = new ConcurrentHashMap<>();
    private final Consumer<ShortMessagePdu> dlrSink;

    public OperatorSmppClientHandler(Consumer<ShortMessagePdu> dlrSink) {
        this.dlrSink = dlrSink;
    }

    /**
     * Регистрирует ожидание ответа на данный sequence_number перед отправкой запроса.
     *
     * <p>CODE_REVIEW.md HIGH #2 (было: {@code client/OperatorSmppClientHandler.java:21} —
     * запись в {@link #pendingResponses} снималась только по совпадающему ответу или по
     * полному {@link #exceptionCaught}; submit/bind, который тайм-аутился без разрыва
     * канала — конгестия/тихая потеря ответа оператором, ровно тот failure mode, который
     * этот сервис обязан переживать — оставлял {@link CompletableFuture} в карте навсегда).
     * Теперь future сам себя завершает по {@link CompletableFuture#orTimeout} через
     * {@code timeoutMs}, и {@code whenComplete} снимает запись из карты при ЛЮБОМ исходе —
     * успех, {@link #exceptionCaught} или таймаут — используя conditional remove(key, value),
     * чтобы не задеть новую запись, если sequence_number успел переиспользоваться
     * (генератор оборачивается) до того, как отработал таймаут старой.
     */
    public CompletableFuture<Pdu> expectResponse(int sequenceNumber, long timeoutMs) {
        CompletableFuture<Pdu> future = new CompletableFuture<>();
        pendingResponses.put(sequenceNumber, future);
        future.whenComplete((pdu, ex) -> pendingResponses.remove(sequenceNumber, future));
        return future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Для метрик/тестов (CODE_REVIEW.md #2) — сколько submit/bind сейчас реально ждут ответа. */
    int pendingResponseCount() {
        return pendingResponses.size();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        Pdu pdu = PduCodec.decode(frame);
        int commandId = pdu.header().commandId();
        int seq = pdu.header().sequenceNumber();

        if (commandId == CommandId.DELIVER_SM) {
            // DLR (или MO) от оператора — handle_raw_dlr.
            if (dlrSink != null) {
                dlrSink.accept((ShortMessagePdu) pdu.body());
            }
            respond(ctx, CommandId.DELIVER_SM_RESP, CommandStatus.ESME_ROK, seq, new ShortMessagePduResp(""));
            return;
        }

        if (commandId == CommandId.ENQUIRE_LINK) {
            respond(ctx, CommandId.ENQUIRE_LINK_RESP, CommandStatus.ESME_ROK, seq, null);
            return;
        }

        // bind_transceiver_resp / submit_sm_resp / enquire_link_resp / unbind_resp —
        // корреляция по sequence_number с ожидающим вызовом клиента.
        CompletableFuture<Pdu> pending = pendingResponses.remove(seq);
        if (pending != null) {
            pending.complete(pdu);
        }
    }

    private void respond(ChannelHandlerContext ctx, int commandId, int status, int seq, Object body) {
        Pdu resp = body == null ? Pdu.headerOnly(commandId, status, seq) : Pdu.withBody(commandId, status, seq, body);
        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(resp, out);
        ctx.writeAndFlush(out);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        pendingResponses.values().forEach(f -> f.completeExceptionally(cause));
        pendingResponses.clear();
        ctx.close();
    }
}
