package uz.mpp.partnersmpp.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import uz.mpp.partnersmpp.admission.AdmissionGate;
import uz.mpp.partnersmpp.core.TokenBucket;
import uz.mpp.partnersmpp.kafkaio.IncomingPublishFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP-сервер SMPP bind'ов (services_specifictaion.md §2.2: StatefulSet, TCP
 * socket остаётся внутри инстанса). Один {@link SmppServerHandler} на
 * каждое соединение — своя сессия/rate limiter.
 */
public final class PartnerSmppServer {

    /**
     * HIGH находка кодревью #4 — без {@code enquire_link} или любых других
     * данных за это время соединение считается мёртвым и закрывается
     * ({@code ReadTimeoutHandler}, реагирует {@code exceptionCaught} в
     * {@code SmppServerHandler}). Конкретное число нигде не зафиксировано
     * документами — разумный запас (типичный {@code enquire_link_interval} —
     * десятки секунд, здесь запас в несколько раз).
     */
    private static final int READ_TIMEOUT_SECONDS = 120;

    /** HIGH находка кодревью #4 — потолок одновременных TCP-соединений на инстанс, разумное, не найденное в документах число. */
    private static final int MAX_CONNECTIONS = 1024;

    private final PartnerAuthenticator authenticator;
    private final IncomingPublishFunction incomingSink;
    private final double rateLimitTps;
    private final AdmissionGate admissionGate;

    private final ChannelRegistry channelRegistry;
    private final SmppServerHandler.BindListener bindListener;
    private final SmppServerHandler.UnbindListener unbindListener;
    private final AtomicInteger activeConnections = new AtomicInteger();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public PartnerSmppServer(PartnerAuthenticator authenticator, IncomingPublishFunction incomingSink,
                             double rateLimitTps, AdmissionGate admissionGate) {
        this(authenticator, incomingSink, rateLimitTps, admissionGate, new ChannelRegistry());
    }

    public PartnerSmppServer(PartnerAuthenticator authenticator, IncomingPublishFunction incomingSink,
                              double rateLimitTps, AdmissionGate admissionGate, ChannelRegistry channelRegistry) {
        this(authenticator, incomingSink, rateLimitTps, admissionGate, channelRegistry, null, null);
    }

    public PartnerSmppServer(PartnerAuthenticator authenticator, IncomingPublishFunction incomingSink,
                              double rateLimitTps, AdmissionGate admissionGate, ChannelRegistry channelRegistry,
                              SmppServerHandler.BindListener bindListener, SmppServerHandler.UnbindListener unbindListener) {
        this.authenticator = authenticator;
        this.incomingSink = incomingSink;
        this.rateLimitTps = rateLimitTps;
        this.admissionGate = admissionGate;
        this.channelRegistry = channelRegistry;
        this.bindListener = bindListener;
        this.unbindListener = unbindListener;
    }

    public ChannelRegistry channelRegistry() {
        return channelRegistry;
    }

    public int start(int port) throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ConnectionLimitHandler(activeConnections, MAX_CONNECTIONS));
                    ch.pipeline().addLast(new ReadTimeoutHandler(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                    ch.pipeline().addLast(new SmppFrameDecoder());
                    ch.pipeline().addLast(new SmppServerHandler(
                        authenticator, incomingSink,
                        new TokenBucket(rateLimitTps, rateLimitTps, System.currentTimeMillis()),
                        admissionGate, channelRegistry, bindListener, unbindListener
                    ));
                }
            });

        serverChannel = bootstrap.bind(port).sync().channel();
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workerGroup != null) {
            // Wait until child-channel channelInactive callbacks have removed
            // their Redis registrations before Main closes the Redis client.
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
