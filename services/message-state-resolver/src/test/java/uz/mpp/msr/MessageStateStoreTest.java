package uz.mpp.msr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class MessageStateStoreTest {

    @Test
    void unknownMessageIdReturnsFreshState() {
        MessageStateStore store = new MessageStateStore();
        LifecycleState state = store.get("never-seen");
        assertNull(state.status());
        assertEquals(0, state.lifecycleVersion());
    }

    @Test
    void putThenGetRoundTrips() {
        MessageStateStore store = new MessageStateStore();
        LifecycleState state = new LifecycleState(LifecycleStatus.SUBMITTED, 1, "e1");
        store.put("m1", state);
        assertEquals(state, store.get("m1"));
    }
}
