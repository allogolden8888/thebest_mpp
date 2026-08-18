package uz.mpp.billing;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String tariffPath = System.getenv().getOrDefault("BILLING_TARIFF_PATH",
            "../../config_schemas/examples/billing_tariff.valid.json");
        TariffResolver defaultTariffResolver = TariffResolver.fromFile(Path.of(tariffPath));
        BillingService billingService = new BillingService(defaultTariffResolver);

        // TariffCache — Фаза 5a плана закрытия API-пробелов (multi-tenancy в
        // Billing Service): per-partner тариф из Configuration Redis, с
        // graceful fallback на defaultTariffResolver для партнёров, для
        // которых BILLING_TARIFF ещё не опубликован через configuration-service
        // (см. TariffCache javadoc).
        TariffCache tariffCache = new TariffCache(RedisUrl.buildConfigurationUrl(), defaultTariffResolver);

        HealthServer health = new HealthServer(9090);
        health.start();

        BillingAccountStore accountStore = new BillingAccountStore(RedisUrl.buildBillingUrl());

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        KafkaConsumer<String, byte[]> consumer = KafkaIo.buildConsumer(bootstrapServers, "billing-service");
        KafkaProducer<String, byte[]> producer = KafkaIo.buildProducer(bootstrapServers);

        // readyz флипается true только после того, как все клиенты (Kafka/Redis)
        // сконструированы — кодревью: "/readyz флипается true до конструирования
        // Redis store/Kafka consumer/producer". Важная оговорка: Kafka/Lettuce
        // клиенты ленивые — "сконструирован" не значит "реально достижим", полная
        // readiness-проверка (например Redis PING) не реализована в этом срезе.
        health.ready.set(true);

        // development_plan.md 5.4 — recurring billing (alphaname/short number
        // monthly fee + service SMS package), см. RecurringBillingJob javadoc.
        // Опционально: без PARTNER_CONFIG_PATH или без recurring_charges в
        // тарифе job просто ничего не планирует на каждом тике (RecurringCharges.plan
        // возвращает пустой список), не падает.
        //
        // Явно НЕ multi-partner в этом проходе (Фаза 5a документирует это как
        // отдельную, не обязательную для разблокировки Ф5 работу — итерация
        // по ВСЕМ партнёрам, не resolve-по-требованию): один статический
        // PARTNER_CONFIG_PATH, account_id = partnerId (Фаза 5a: 1:1 —
        // BILLING_ACCOUNT_ID больше не существует как отдельный, рассинхронизируемый
        // с partner_id env var).
        String partnerConfigPath = System.getenv().getOrDefault("PARTNER_CONFIG_PATH",
            "../../config_schemas/examples/partner.valid.json");
        PartnerSendersResolver.PartnerSenders partnerSenders = PartnerSendersResolver.fromFile(Path.of(partnerConfigPath));
        RecurringBillingJob recurringBillingJob = new RecurringBillingJob(
            accountStore, partnerSenders.partnerId(), partnerSenders.partnerId(), partnerSenders.senders(),
            defaultTariffResolver.alphanameMonthlyFee(), defaultTariffResolver.servicePackage(), Clock.systemUTC());
        // Идемпотентно (charge_id-дедуп) — ежедневный тик, не точный
        // "1-го числа каждого месяца" cron; проще и надёжнее восстанавливается
        // после простоя/рестарта пода (следующий тик подхватит пропущенные
        // charge за текущий период).
        ScheduledExecutorService recurringScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "billing-recurring-charges");
            t.setDaemon(true);
            return t;
        });
        // ScheduledExecutorService.scheduleAtFixedRate молча ПРЕКРАЩАЕТ все
        // будущие запуски, если Runnable хоть раз бросит необработанное
        // исключение — RecurringBillingJob.run() уже ловит RuntimeException
        // на каждый charge отдельно, но эта внешняя catch(Throwable) — второй
        // рубеж (Error, программная ошибка и т.п.), чтобы recurring billing
        // не остановился навсегда молча из-за одного бага.
        recurringScheduler.scheduleAtFixedRate(() -> {
            try {
                recurringBillingJob.run();
            } catch (Throwable t) {
                java.util.logging.Logger.getLogger(Main.class.getName())
                    .log(java.util.logging.Level.SEVERE, t, () -> "recurring billing tick failed целиком, будет повторено через 24ч");
            }
        }, 0, 1, TimeUnit.DAYS);

        AtomicBoolean running = new AtomicBoolean(true);
        // KafkaConsumer не потокобезопасен — close() из другого потока, пока
        // основной поток внутри poll(), некорректен. wakeup() — единственный
        // потокобезопасный способ прервать блокирующий poll() извне; сам
        // KafkaIo.run() ловит WakeupException и закрывает consumer/producer
        // из СВОЕГО потока (см. KafkaIo.java).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            consumer.wakeup();
            health.stop();
            recurringScheduler.shutdownNow();
            accountStore.close();
            tariffCache.close();
        }, "billing-service-shutdown"));

        // Реальная находка (нагрузочный прогон, 1000 msg/s): раньше
        // необработанное исключение из KafkaIo.run() (например
        // CommitFailedException — consumer выброшен из группы за
        // превышение max.poll.interval.ms) просто вываливалось из main() и
        // печаталось как "Exception in thread main", но JVM НЕ завершался —
        // HealthServer держит внутренний com.sun.net.httpserver.HttpServer
        // с НЕ-daemon потоком "HTTP-Dispatcher" (публичного API сделать его
        // daemon нет, setExecutor() влияет только на обработчики хендлеров,
        // не на сам dispatcher-поток), так что процесс оставался "живым"
        // (docker ps: Up) с полностью мёртвым consumer'ом, при этом
        // /healthz и /readyz продолжали отвечать 200 — невидимый полный
        // отказ. System.exit() — единственный надёжный способ гарантированно
        // завершить JVM независимо от того, какие ещё не-daemon потоки
        // библиотеки создают; restart-policy в docker-compose.yml поднимет
        // контейнер заново после этого.
        try {
            KafkaIo.run(consumer, producer, accountStore, billingService, tariffCache, running);
            consumer.close();
            producer.close();
        } catch (Throwable t) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, t,
                () -> "billing-service Kafka consumer loop упал фатально — принудительно завершаем "
                    + "процесс (не остаёмся zombie-процессом с живым /healthz у мёртвого consumer'а)");
            System.exit(1);
        }
    }
}
