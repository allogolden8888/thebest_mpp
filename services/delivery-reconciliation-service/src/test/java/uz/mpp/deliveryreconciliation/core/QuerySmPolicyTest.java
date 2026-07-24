package uz.mpp.deliveryreconciliation.core;

import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.Protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QuerySmPolicyTest {

    @Test
    void disabledForHttpRegardlessOfConfig() {
        assertEquals(QuerySmPolicy.Decision.DISABLED, QuerySmPolicy.check(Protocol.PROTOCOL_HTTP, true));
    }

    @Test
    void disabledForSmppWhenConfigDisables() {
        assertEquals(QuerySmPolicy.Decision.DISABLED, QuerySmPolicy.check(Protocol.PROTOCOL_SMPP, false));
    }

    @Test
    void enabledForSmppWhenConfigEnables() {
        assertEquals(QuerySmPolicy.Decision.ENABLED, QuerySmPolicy.check(Protocol.PROTOCOL_SMPP, true));
    }
}
