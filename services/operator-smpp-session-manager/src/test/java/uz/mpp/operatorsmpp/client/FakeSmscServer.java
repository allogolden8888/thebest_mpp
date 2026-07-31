package uz.mpp.operatorsmpp.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import uz.mpp.operatorsmpp.codec.*;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Тестовый "фейковый оператор" (SMSC) — реальный Netty TCP-сервер на
 * localhost, не мок: принимает bind_transceiver, отвечает на submit_sm
 * фиксированным message_id, может пушить deliver_sm (DLR) в активный канал
 * по требованию теста. Используется только в тестах {@link OperatorSmppClient}.
 */
public final class FakeSmscServer {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final AtomicReference<Channel> lastChannel = new AtomicReference<>();
    private volatile boolean dropSubmitResponses = false;

    public int start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    lastChannel.set(ch);
                    ch.pipeline().addLast(new SmppFrameDecoder());
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
                            Pdu pdu = PduCodec.decode(frame);
                            int seq = pdu.header().sequenceNumber();
                            switch (pdu.header().commandId()) {
                                case CommandId.BIND_TRANSCEIVER ->
                                    respond(ctx, CommandId.BIND_TRANSCEIVER_RESP, CommandStatus.ESME_ROK, seq,
                                        new BindTransceiverResp(((BindTransceiver) pdu.body()).systemId()));
                                case CommandId.SUBMIT_SM -> {
                                    // dropSubmitResponses имитирует реальный failure mode SMSC
                                    // из CODE_REVIEW.md finding #2 — "тихая" потеря ответа
                                    // без обрыва TCP-канала, не только явный timeout по TPS.
                                    if (!dropSubmitResponses) {
                                        respond(ctx, CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_ROK, seq,
                                            new ShortMessagePduResp("smsc-msg-1"));
                                    }
                                }
                                case CommandId.ENQUIRE_LINK ->
                                    respond(ctx, CommandId.ENQUIRE_LINK_RESP, CommandStatus.ESME_ROK, seq, null);
                                case CommandId.DELIVER_SM_RESP -> {
                                    // ack на наш пуш deliver_sm — ничего не делаем
                                }
                                default ->
                                    respond(ctx, CommandId.GENERIC_NACK, CommandStatus.ESME_RINVCMDID, seq, null);
                            }
                        }
                    });
                }
            });

        serverChannel = bootstrap.bind(0).sync().channel();
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /** CODE_REVIEW.md finding #2 (тест) — не отвечать на submit_sm, имитируя тихую потерю ответа SMSC. */
    public void setDropSubmitResponses(boolean drop) {
        this.dropSubmitResponses = drop;
    }

    /**
     * CODE_REVIEW.md CRITICAL #1 (тест) — обрывает TCP-соединение с текущим клиентом,
     * не останавливая сам сервер (который продолжает слушать и примет reconnect).
     * Симулирует сетевой блип/рестарт SMSC, а не graceful close с клиентской стороны.
     */
    public void disconnectClient() {
        Channel ch = lastChannel.get();
        if (ch != null) {
            ch.close();
        }
    }

    public void pushDeliverSm(ShortMessagePdu body, int sequenceNumber) {
        Channel ch = lastChannel.get();
        Pdu pdu = Pdu.withBody(CommandId.DELIVER_SM, CommandStatus.ESME_ROK, sequenceNumber, body);
        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(pdu, out);
        ch.writeAndFlush(out);
    }

    private static void respond(ChannelHandlerContext ctx, int commandId, int status, int seq, Object body) {
        Pdu resp = body == null ? Pdu.headerOnly(commandId, status, seq) : Pdu.withBody(commandId, status, seq, body);
        ByteBuf out = Unpooled.buffer();
        PduCodec.encode(resp, out);
        ctx.writeAndFlush(out);
    }

    public void stop() {
        if (serverChannel != null) serverChannel.close();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}