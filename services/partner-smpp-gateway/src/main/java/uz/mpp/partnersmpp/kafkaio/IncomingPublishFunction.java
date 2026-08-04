package uz.mpp.partnersmpp.kafkaio;

import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.util.function.BiConsumer;

/**
 * Функциональный тип для {@link IncomingPublisher#publish(IncomingMessage, BiConsumer)}
 * — введён для исправления HIGH находки кодревью #2 (submit_sm_resp
 * ESME_ROK отправлялся до подтверждения Kafka-публикации). {@code callback}
 * вызывается ровно один раз: {@code (null, null)} на успех, {@code (null,
 * exception)} на ошибку.
 */
@FunctionalInterface
public interface IncomingPublishFunction {
    void publish(IncomingMessage message, BiConsumer<Void, Throwable> callback);
}
