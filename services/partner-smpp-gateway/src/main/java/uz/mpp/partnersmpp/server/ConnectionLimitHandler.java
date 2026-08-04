package uz.mpp.partnersmpp.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Закрывает HIGH находку кодревью (CODE_REVIEW.md, "partner-smpp-gateway"
 * #4): раньше не было ни {@code IdleStateHandler}/{@code ReadTimeoutHandler}
 * (см. {@code PartnerSmppServer} — теперь {@code ReadTimeoutHandler} первым
 * в пайплайне), ни ограничения на число одновременных соединений — пир,
 * открывающий много raw TCP-соединений и ничего не отправляющий, занимал
 * канал+FD бессрочно. Первый хендлер в пайплайне (до frame decoder'а) —
 * решение "принять/отклонить" не должно ждать ни байта протокольных данных.
 */
public final class ConnectionLimitHandler extends ChannelInboundHandlerAdapter {

    private final AtomicInteger activeConnections;
    private final int maxConnections;

    public ConnectionLimitHandler(AtomicInteger activeConnections, int maxConnections) {
        this.activeConnections = activeConnections;
        this.maxConnections = maxConnections;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (activeConnections.incrementAndGet() > maxConnections) {
            activeConnections.decrementAndGet();
            ctx.close();
            return;
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        activeConnections.decrementAndGet();
        super.channelInactive(ctx);
    }
}
