package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PacerCore — чистая two-phase HTB dispatch-логика, см. класс-javadoc.
 * TPS=100 везде ниже (70/20/10 = HIGH/MEDIUM/LOW), buckets сконструированы
 * через "сырой" {@link TokenBucket#TokenBucket(double, double, long)}
 * (capacity==rate, БЕЗ burst-headroom урезания) — специально, чтобы
 * изолировать 70/20/10-математику PacerCore от находки про мгновенный
 * burst (та уже отдельно покрыта TokenBucketTest.withBurstHeadroom*).
 */
class PacerCoreTest {

    private static final long NOW = 0L;

    private static PacerCore pacerAtFullCapacity(double tps) {
        TokenBucket ceil = new TokenBucket(tps, tps, NOW);
        TokenBucket high = new TokenBucket(tps * PacerCore.HIGH_SHARE, tps * PacerCore.HIGH_SHARE, NOW);
        TokenBucket medium = new TokenBucket(tps * PacerCore.MEDIUM_SHARE, tps * PacerCore.MEDIUM_SHARE, NOW);
        TokenBucket low = new TokenBucket(tps * PacerCore.LOW_SHARE, tps * PacerCore.LOW_SHARE, NOW);
        return new PacerCore(ceil, high, medium, low);
    }

    private static Map<PriorityTier, Integer> demand(int high, int medium, int low) {
        Map<PriorityTier, Integer> d = new EnumMap<>(PriorityTier.class);
        d.put(PriorityTier.HIGH, high);
        d.put(PriorityTier.MEDIUM, medium);
        d.put(PriorityTier.LOW, low);
        return d;
    }

    @Test
    void exactSplitAtKnownTpsWhenAllTiersSaturated() {
        // TPS=100, все три tier'а хотят больше своей доли -> фаза 1 отдаёт
        // ровно 70/20/10, ceilBucket полностью исчерпан фазой 1 (100=100),
        // фазе 2 нечего добавить ни одному tier'у.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(10_000, 10_000, 10_000), 100_000, NOW);

        assertEquals(70, plan.forTier(PriorityTier.HIGH), "HIGH должен получить ровно TPS_LIMIT*0.70=70");
        assertEquals(20, plan.forTier(PriorityTier.MEDIUM), "MEDIUM должен получить ровно TPS_LIMIT*0.20=20");
        assertEquals(10, plan.forTier(PriorityTier.LOW), "LOW должен получить ровно TPS_LIMIT*0.10=10");
        assertEquals(100, plan.total(), "суммарно не больше TPS_LIMIT — иначе фаза 2 задвоила бы burst, который весь пейсер должен устранить");
    }

    @Test
    void guaranteedFloorServedUnderSaturationOfHigherTiers() {
        // HIGH и MEDIUM насыщены (хотят намного больше своей доли), но LOW
        // тоже что-то просит (пусть немного) — гарантированная доля LOW
        // должна быть обслужена, а не съедена HIGH/MEDIUM.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(10_000, 10_000, 5), 100_000, NOW);

        assertEquals(5, plan.forTier(PriorityTier.LOW), "LOW должен получить весь свой (небольшой) спрос несмотря на насыщенные HIGH/MEDIUM");
        assertTrue(plan.forTier(PriorityTier.HIGH) >= 70, "HIGH не должен быть урезан ниже своей гарантии из-за LOW");
    }

    @Test
    void highBorrowsBeyondItsShareWhenOthersAreIdle() {
        // MEDIUM и LOW полностью простаивают (спрос 0) -> весь ceilBucket
        // (100) доступен HIGH через заимствование фазы 2 поверх его
        // гарантированных 70.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(10_000, 0, 0), 100_000, NOW);

        assertEquals(100, plan.forTier(PriorityTier.HIGH), "простаивающие MEDIUM/LOW должны отдать весь запас HIGH: 70 гарантии + 30 заимствования");
        assertEquals(0, plan.forTier(PriorityTier.MEDIUM));
        assertEquals(0, plan.forTier(PriorityTier.LOW));
    }

    @Test
    void idleHighCapacityFlowsDownToMediumAndLow() {
        // HIGH простаивает (спрос 0) -> его 70 гарантированных токенов
        // никогда не расходуются его собственным bucket'ом, а
        // освободившийся ceilBucket достаётся MEDIUM/LOW через
        // заимствование, в порядке приоритета.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(0, 10_000, 10_000), 100_000, NOW);

        assertEquals(0, plan.forTier(PriorityTier.HIGH));
        assertTrue(plan.forTier(PriorityTier.MEDIUM) > 20, "MEDIUM должен получить больше своей гарантии за счёт простаивающего HIGH");
        assertEquals(100, plan.forTier(PriorityTier.HIGH) + plan.forTier(PriorityTier.MEDIUM) + plan.forTier(PriorityTier.LOW),
            "суммарно освободившаяся от HIGH ёмкость не должна пропадать — MEDIUM+LOW должны выбрать весь TPS_LIMIT");
    }

    @Test
    void borrowingOffersPartialHeadroomToHigherPriorityTierFirst() {
        // HIGH просит МЕНЬШЕ своей гарантии (60 из 70) -> фаза 1 тратит
        // 60+20+10=90 из ceilBucket=100, оставляя ровно 10 "частичного"
        // запаса на фазу 2. И MEDIUM, и LOW всё ещё хотят больше — по
        // приоритету запас должен уйти MEDIUM целиком, LOW — ничего сверх
        // своей гарантии.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(60, 10_000, 10_000), 100_000, NOW);

        assertEquals(60, plan.forTier(PriorityTier.HIGH), "HIGH полностью удовлетворён на 60, не участвует в фазе 2");
        assertEquals(30, plan.forTier(PriorityTier.MEDIUM), "MEDIUM должен получить свою гарантию (20) + весь оставшийся частичный запас (10) = 30");
        assertEquals(10, plan.forTier(PriorityTier.LOW), "LOW должен остаться ровно на своей гарантии (10) — запас в фазе 2 уже забрал MEDIUM как более приоритетный");
    }

    @Test
    void totalAdmittedNeverExceedsAvailablePermitsRegardlessOfBucketCapacity() {
        // Даже если все bucket'ы (включая ceilBucket) полны и спрос
        // огромен, семафор конкурентности (переданный как availablePermits)
        // должен быть жёстким верхним пределом суммарной диспетчеризации
        // за один тик.
        PacerCore pacer = pacerAtFullCapacity(100);
        PacerCore.AdmitPlan plan = pacer.decide(demand(10_000, 10_000, 10_000), 7, NOW);

        assertEquals(7, plan.total(), "суммарное количество допущенных элементов не должно превышать доступные permits");
    }
}
