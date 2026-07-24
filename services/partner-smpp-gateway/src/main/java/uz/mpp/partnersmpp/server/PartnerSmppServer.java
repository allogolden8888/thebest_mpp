package uz.mpp.partnersmpp.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import uz.mpp.partnersmpp.core.TokenBucket;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.util.function.Consumer;

/**
 * TCP-сервер SMPP bind'ов (services_specifictaion.md §2.2: StatefulSet, TCP
 * socket остаётся внутри инстанса). Один {@link SmppServerHandler} на
 * каждое соединение — своя сессия/rate limiter.
 */
public final class PartnerSmppServer {

    private final PartnerAuthenticator authenticator;
    private final Consumer<IncomingMessage> incomingSink;
    private final double rateLimitTps;

    private final ChannelRegistry channelRegistry;
    private final SmppServerHandler.BindListener bindListener;
    private final SmppServerHandler.UnbindListener unbindListener;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public PartnerSmppServer(PartnerAuthenticator authenticator, Consumer<IncomingMessage> incomingSink, double rateLimitTps) {
        this(authenticator, incomingSink, rateLimitTps, new ChannelRegistry());
    }

    public PartnerSmppServer(PartnerAuthenticator authenticator, Consumer<IncomingMessage> incomingSink,
                              double rateLimitTps, ChannelRegistry channelRegistry) {
        this(authenticator, incomingSink, rateLimitTps, channelRegistry, null, null);
    }

    public PartnerSmppServer(PartnerAuthenticator authenticator, Consumer<IncomingMessage> incomingSink,
                              double rateLimitTps, ChannelRegistry channelRegistry,
                              SmppServerHandler.BindListener bindListener, SmppServerHandler.UnbindListener unbindListener) {
        this.authenticator = authenticator;
        this.incomingSink = incomingSink;
        this.rateLimitTps = rateLimitTps;
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
                    ch.pipeline().addLast(new SmppFrameDecoder());
                    ch.pipeline().addLast(new SmppServerHandler(
                        authenticator, incomingSink,
                        new TokenBucket(rateLimitTps, rateLimitTps, System.currentTimeMillis()),
                        channelRegistry, bindListener, unbindListener
                    ));
                }
            });

        serverChannel = bootstrap.bind(port).sync().channel();
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}