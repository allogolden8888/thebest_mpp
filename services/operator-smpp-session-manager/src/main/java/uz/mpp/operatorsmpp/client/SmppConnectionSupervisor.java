package uz.mpp.operatorsmpp.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Следит за живостью SMPP-сессии к оператору и переподключается после обрыва,
 * произошедшего ПОСЛЕ старта — reconnect (service_internal_methods.md §1.3).
 *
 * <p>CODE_REVIEW.md CRITICAL #1: раньше {@code Main.java::connectAndBindWithRetry}
 * запускался ровно один раз, при старте процесса ({@code Main.java:98-118}); ничто не
 * связывало более поздний обрыв канала с повторной попыткой connect+bind, а
 * {@code health.setReady(true)} выставлялся один раз и никогда не переоценивался
 * против {@code client.isActive()}. Грациозный рестарт SMSC или сетевой сбой навсегда
 * "чернили" все submit по этому оператору до ручного рестарта пода — при том, что
 * `reconnect` прямо в обязанностях сервиса.
 *
 * <p>Механизм: {@link OperatorSmppClient#onDisconnect} вешает слушателя на
 * {@code channel.closeFuture()} текущего канала (обрыв — TCP FIN/RST от оператора,
 * либо {@code ctx.close()} из {@link OperatorSmppClientHandler#exceptionCaught} на
 * malformed PDU, finding #4). При срабатывании: readiness сразу переводится в false,
 * а сама повторная попытка connect+bind (в т.ч. её backoff-паузы) уходит в отдельный
 * однопоточный executor, чтобы не блокировать Netty event-loop поток, на котором
 * сработал {@code closeFuture}. После успешного reconnect слушатель перевешивается на
 * НОВЫЙ канал — {@code closeFuture} привязан к конкретному объекту channel и не
 * переживает reconnect автоматически.
 */
public final class SmppConnectionSupervisor {

    /** Выполняет полный connect+bind (обычно с собственной retry-петлёй/backoff внутри). */
    @FunctionalInterface
    public interface Connector {
        void connectAndBind() throws InterruptedException;
    }

    private final OperatorSmppClient client;
    private final Connector connector;
    private final Consumer<Boolean> onReadyChange;
    private final ExecutorService reconnectExecutor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public SmppConnectionSupervisor(OperatorSmppClient client, Connector connector, Consumer<Boolean> onReadyChange) {
        this.client = client;
        this.connector = connector;
        this.onReadyChange = onReadyChange;
        this.reconnectExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "smpp-reconnect");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Вешает обработчик разрыва на ТЕКУЩИЙ канал клиента. Вызывать сразу после каждого
     * успешного connect+bind — и первого, при старте, и каждого последующего reconnect.
     */
    public void arm() {
        client.onDisconnect(() -> {
            if (stopped.get()) {
                return;
            }
            onReadyChange.accept(false);
            reconnectExecutor.submit(() -> {
                if (stopped.get()) {
                    return;
                }
                try {
                    connector.connectAndBind();
                    onReadyChange.accept(true);
                    arm();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        });
    }

    /** Останавливает supervisor — вызывать при shutdown, до {@code client.close()}. */
    public void stop() {
        stopped.set(true);
        reconnectExecutor.shutdownNow();
    }
}
