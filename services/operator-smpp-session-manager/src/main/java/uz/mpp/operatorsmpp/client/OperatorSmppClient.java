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
import uz.mpp.operatorsmpp.session.SequenceNumberGenerator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

    private EventLoopGroup group;
    private Channel channel;
    private OperatorSmppClientHandler handler;

    public OperatorSmppClient(Consumer<ShortMessagePdu> dlrSink) {
        this.dlrSink = dlrSink;
    }

    public void connect(String host, int port) throws InterruptedException {
        group = new NioEventLoopGroup();
        handler = new OperatorSmppClientHandler(dlrSink);

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

        channel = bootstrap.connect(host, port).sync().channel();
    }

    /** bind_operator — синхронный bind_transceiver, ждёт ответ до timeoutMs. */
    public Pdu bind(String systemId, String password, String systemType, long timeoutMs) throws Exception {
        int seq = sequenceNumberGenerator.next();
        CompletableFuture<Pdu> future = handler.expectResponse(seq);
        write(Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, seq,
            new BindTransceiver(systemId, password, systemType, (byte) 0x34, (byte) 0, (byte) 0, "")));
        return await(future, timeoutMs);
    }

    /** send_submit_sm — публикует segment оператору, ждёт submit_sm_resp. */
    public Pdu submitSm(ShortMessagePdu body, long timeoutMs) throws Exception {
        int seq = sequenceNumberGenerator.next();
        CompletableFuture<Pdu> future = handler.expectResponse(seq);
        write(Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, seq, body));
        return await(future, timeoutMs);
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

    private static Pdu await(CompletableFuture<Pdu> future, long timeoutMs) throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
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