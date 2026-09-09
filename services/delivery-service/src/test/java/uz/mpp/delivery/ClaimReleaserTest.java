package uz.mpp.delivery;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Освобождение ключей {@code dlvsubmit:{stage_execution_id}} — проверяется
 * {@code collectReleasable}, чистая часть {@link KafkaIo.ClaimReleaser}:
 * решение «какие ключи уже безопасно отпустить» принимается именно тут, а
 * сам {@code UNLINK} — тривиальная отправка команды.
 *
 * <p>Цена ошибки здесь несимметрична: не освободить ключ — это лишняя память
 * до истечения TTL, а освободить рано — это ПОВТОРНЫЙ SMS абоненту, потому
 * что передоставленная запись получит от {@code claim} вердикт {@code Won}.
 * Поэтому тесты сторожат именно раннее освобождение.
 */
class ClaimReleaserTest {

    private static final TopicPartition P0 = new TopicPartition(KafkaIo.INPUT_TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(KafkaIo.INPUT_TOPIC, 1);

    private static KafkaIo.ClaimReleaser releaser() {
        // store не участвует: collectReleasable до него не доходит.
        return new KafkaIo.ClaimReleaser(null);
    }

    @Test
    void освобождаетТолькоОффсетыСтрогоДоЗакоммиченнойПозиции() {
        KafkaIo.ClaimReleaser releaser = releaser();
        releaser.track(P0, 10L, "sx-10");
        releaser.track(P0, 11L, "sx-11");
        releaser.track(P0, 12L, "sx-12");

        // Коммитится позиция 12 — «следующий к вычитыванию». Значит записи 10
        // и 11 закоммичены, а 12 ещё нет и может передоставиться.
        List<String> released = releaser.collectReleasable(Map.of(P0, 12L));

        assertEquals(List.of("sx-10", "sx-11"), released);
        assertEquals(1, releaser.pendingCount(), "ключ незакоммиченной записи обязан остаться");
    }

    @Test
    void повторныйВызовНеОтдаётУжеОсвобождённое() {
        KafkaIo.ClaimReleaser releaser = releaser();
        releaser.track(P0, 5L, "sx-5");

        assertEquals(List.of("sx-5"), releaser.collectReleasable(Map.of(P0, 6L)));
        assertTrue(releaser.collectReleasable(Map.of(P0, 6L)).isEmpty(),
            "второй UNLINK тех же ключей — бессмысленный трафик к Redis");
        assertEquals(0, releaser.pendingCount());
    }

    @Test
    void партицииНезависимы() {
        KafkaIo.ClaimReleaser releaser = releaser();
        releaser.track(P0, 1L, "p0-1");
        releaser.track(P1, 1L, "p1-1");

        assertEquals(List.of("p0-1"), releaser.collectReleasable(Map.of(P0, 2L)));
        assertEquals(1, releaser.pendingCount(), "продвижение P0 не смеет освобождать ключи P1");
    }

    @Test
    void записиБезClaimНеОтслеживаются() {
        KafkaIo.ClaimReleaser releaser = releaser();
        // HOLD / sandbox / ранний REJECTED — claim не вызывался, освобождать нечего.
        releaser.track(P0, 1L, null);
        releaser.track(P0, 2L, "sx-2");

        assertEquals(1, releaser.pendingCount());
        assertEquals(List.of("sx-2"), releaser.collectReleasable(Map.of(P0, 3L)));
    }

    @Test
    void отобраннаяРебалансомПартицияНеОсвобождается() {
        KafkaIo.ClaimReleaser releaser = releaser();
        releaser.track(P0, 1L, "sx-1");
        releaser.track(P1, 1L, "p1-1");

        // Записи отобранной партиции передоставятся новому владельцу — ключ
        // там единственное, что удержит его от повторного submit'а.
        releaser.forget(List.of(P0));

        assertTrue(releaser.collectReleasable(Map.of(P0, 99L)).isEmpty());
        assertEquals(1, releaser.pendingCount(), "ключи оставшейся партиции не тронуты");
    }

    @Test
    void неизвестнаяПартицияВКоммитеНеЛомаетОсвобождение() {
        KafkaIo.ClaimReleaser releaser = releaser();
        releaser.track(P0, 1L, "sx-1");

        // Коммит может нести партицию, по которой мы ничего не отслеживали
        // (все её записи шли путём без claim).
        assertEquals(List.of("sx-1"), releaser.collectReleasable(Map.of(P0, 2L, P1, 7L)));
    }
}
