package uz.mpp.scheduler.background;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import uz.mpp.scheduler.background.topology.BackgroundLaneTopology;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduler — Background Lane (services_specifictaion.md §3.1): DLR
 * correlation retry, notification retry, фоновые задачи. Kafka Streams
 * Processor API, exactly_once_v2, RocksDB.
 */
public final class Main {

    public static void main(String[] args) throws IOException {
        AtomicBoolean ready = new AtomicBoolean(false);
        HttpServer healthServer = startHealthServer(ready);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "scheduler-background-lane");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/scheduler-background-lane-state"));

        KafkaStreams streams = new KafkaStreams(BackgroundLaneTopology.build(), props);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                ready.set(true);
            } else if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.PENDING_SHUTDOWN) {
                ready.set(false);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            healthServer.stop(0);
        }));

        streams.start();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }

    private static HttpServer startHealthServer(AtomicBoolean ready) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(9090), 0);

        server.createContext("/healthz", exchange -> {
            byte[] body = "ok".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/readyz", exchange -> {
            byte[] body = ready.get() ? "ready".getBytes() : "not ready".getBytes();
            exchange.sendResponseHeaders(ready.get() ? 200 : 503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/metrics", exchange -> {
            byte[] body = ("# HELP scheduler_background_lane_up Service liveness placeholder\n"
                + "# TYPE scheduler_background_lane_up gauge\nscheduler_background_lane_up 1\n").getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.setExecutor(null);
        server.start();
        return server;
    }
}
