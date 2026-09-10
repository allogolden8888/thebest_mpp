package uz.mpp.operatorsmpp.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import uz.mpp.operatorsmpp.codec.*;
import uz.mpp.operatorsmpp.core.PduLogEvent;
import uz.mpp.operatorsmpp.session.SequenceNumberGenerator;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * bind_operator + send_submit_sm + enquire_link_tick (service_internal_methods.md
 * §1.3) — SMPP-клиент (ESME) к оператору. Netty-клиентский bootstrap на том
 * же smpp-codec, что и Partner SMPP Gateway (не JSMPP/cloudhopper).
 */
public final class OperatorSmppClient {

    private final SequenceNumberGenerator sequenceNumberGenerator = new SequenceNumberGenerator();
    private final Consumer<ShortMessagePdu> dlrSink;
    // per-PDU диагностический след (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40) —
    // отдельный sink от dlrSink: тот несёт только бизнес-DLR (deliver_sm),
    // этот — КАЖДЫЙ submit_sm/submit_sm_resp, реально ушедший на wire.
    private final Consumer<PduLogEvent> pduLogSink;

    private EventLoopGroup group;
    private Channel channel;
    private OperatorSmppClientHandler handler;

    public OperatorSmppClient(Consumer<ShortMessagePdu> dlrSink) {
        this(dlrSink, null);
    }

    public OperatorSmppClient(Consumer<ShortMessagePdu> dlrSink, Consumer<PduLogEvent> pduLogSink) {
        this.dlrSink = dlrSink;
        this.pduLogSink = pduLogSink;
    }

    /**
     * connect — открывает новый TCP-канал к оператору. Вызывается и при первом старте, и
     * при каждой попытке reconnect (CODE_REVIEW.md finding #1, {@code Main.java:98-118}).
     * До фикса каждый повторный вызов при неудачной retry-петле создавал новую
     * {@link NioEventLoopGroup}, теряя ссылку на предыдущую без {@code shutdownGracefully()}
     * — при затяжном отказе SMSC (много попыток reconnect подряд) это утечка потоков ОС.
     * Теперь предыдущая группа (если была) гасится перед созданием новой, а если сам
     * connect не удался — гасится и только что созданная.
     */
    public void connect(String host, int port) throws InterruptedException {
        if (group != null) {
            group.shutdownGracefully();
        }

        EventLoopGroup newGroup = new NioEventLoopGroup();
        group = newGroup;
        handler = new OperatorSmppClientHandler(dlrSink, pduLogSink);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new uz.mpp.operatorsmpp.client.SmppFrameDecoder());
                    ch.pipeline().addLast(handler);
                }
            });

        boolean success = false;
        try {
            channel = bootstrap.connect(host, port).sync().channel();
            success = true;
        } finally {
            if (!success) {
                newGroup.shutdownGracefully();
            }
        }
    }

    /**
     * onDisconnect (CODE_REVIEW.md finding #1) — регистрирует слушателя обрыва ТЕКУЩЕГО
     * TCP-канала. {@link Channel#closeFuture()} срабатывает ровно один раз для канала —
     * будь то graceful close, RST от SMSC или {@code ctx.close()} из
     * {@link OperatorSmppClientHandler#exceptionCaught} на malformed PDU (finding #4).
     * Вызывать заново после каждого успешного (пере)connect — слушатель привязан к
     * конкретному объекту channel, не переживает reconnect автоматически.
     */
    public void onDisconnect(Runnable callback) {
        if (channel == null) {
            throw new IllegalStateException("onDisconnect: connect() ещё не вызывался");
        }
        channel.closeFuture().addListener(future -> callback.run());
    }

    /** bind_operator — синхронный bind_transceiver, ждёт ответ до timeoutMs. */
    public Pdu bind(String systemId, String password, String systemType, long timeoutMs) throws Exception {
        int seq = sequenceNumberGenerator.next();
        CompletableFuture<Pdu> future = handler.expectResponse(seq, timeoutMs);
        write(Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, seq,
            new BindTransceiver(systemId, password, systemType, (byte) 0x34, (byte) 0, (byte) 0, "")));
        return await(future);
    }

    /** send_submit_sm — публикует segment оператору, ждёт submit_sm_resp. Без per-PDU корреляции с сообщением (тесты/внутренние вызовы) — messageId/stageExecutionId пустые в pduLogSink. */
    public Pdu submitSm(ShortMessagePdu body, long timeoutMs) throws Exception {
        return submitSm(body, timeoutMs, "", "");
    }

    /**
     * send_submit_sm с messageId/stageExecutionId для per-PDU лога
     * (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40) — вызывается из
     * {@code OperatorSubmitServer.dispatchOne}, который единственный на
     * этом уровне знает исходный {@code message_id}. sequence_number
     * генерируется здесь же (не выше) — это единственное место, которое
     * реально видит и момент отправки submit_sm, и момент получения
     * submit_sm_resp на один и тот же seq.
     */
    public Pdu submitSm(ShortMessagePdu body, long timeoutMs, String messageId, String stageExecutionId) throws Exception {
        int seq = sequenceNumberGenerator.next();
        if (pduLogSink != null) {
            pduLogSink.accept(new PduLogEvent(PduLogEvent.DIRECTION_A2P, CommandId.name(CommandId.SUBMIT_SM),
                seq, messageId, stageExecutionId, "", "", Instant.now()));
        }
        CompletableFuture<Pdu> future = handler.expectResponse(seq, timeoutMs);
        write(Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, seq, body));
        try {
            Pdu resp = await(future);
            if (pduLogSink != null) {
                String smscMessageId = resp.header().commandStatus() == CommandStatus.ESME_ROK
                    ? ((ShortMessagePduResp) resp.body()).messageId() : "";
                String status = resp.header().commandStatus() == CommandStatus.ESME_ROK
                    ? "OK" : "0x" + Integer.toHexString(resp.header().commandStatus());
                pduLogSink.accept(new PduLogEvent(PduLogEvent.DIRECTION_A2P, CommandId.name(CommandId.SUBMIT_SM_RESP),
                    seq, messageId, stageExecutionId, smscMessageId, status, Instant.now()));
            }
            return resp;
        } catch (TimeoutException e) {
            if (pduLogSink != null) {
                pduLogSink.accept(new PduLogEvent(PduLogEvent.DIRECTION_A2P, CommandId.name(CommandId.SUBMIT_SM_RESP),
                    seq, messageId, stageExecutionId, "", "TIMEOUT", Instant.now()));
            }
            throw e;
        }
    }

    /** enquire_link_tick — таймер поддержания соединения. */
    public void sendEnquireLink() {
        write(Pdu.headerOnly(CommandId.ENQUIRE_LINK, CommandStatus.ESME_ROK, sequenceNumberGenerator.next()));
    }

    private void write(Pdu pdu) {
        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(pdu, out);
        channel.writeAndFlush(out);
    }

    /**
     * Ждёт ответ. Таймаут теперь применяется внутри самого future
     * ({@link OperatorSmppClientHandler#expectResponse} -> {@code orTimeout}, CODE_REVIEW.md
     * finding #2), а не отдельным {@code get(timeout, unit)} здесь — так future реально
     * доводится до завершения и снимает себя из {@code pendingResponses}, а не просто
     * "бросается" вызывающим кодом. {@code get()} без аргументов при этом бросает
     * {@link ExecutionException} с {@link TimeoutException} внутри вместо голого
     * {@link TimeoutException}, поэтому разворачиваем его здесь — чтобы контракт метода
     * (голый {@code TimeoutException} наружу) не поменялся для вызывающего кода
     * ({@code OperatorSubmitServer.submit} ловит {@code TimeoutException} напрямую).
     */
    private static Pdu await(CompletableFuture<Pdu> future) throws ExecutionException, InterruptedException, TimeoutException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException timeoutException) {
                throw timeoutException;
            }
            throw e;
        }
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    /** Для метрик/тестов (CODE_REVIEW.md #2) — сколько submit/bind сейчас ждут ответа оператора. */
    public int pendingResponseCount() {
        return handler == null ? 0 : handler.pendingResponseCount();
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}