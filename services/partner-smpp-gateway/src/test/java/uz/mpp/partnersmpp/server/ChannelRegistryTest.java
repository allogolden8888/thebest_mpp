package uz.mpp.partnersmpp.server;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ChannelRegistryTest {

    @Test
    void registerThenLookupReturnsSameChannelAndEpoch() {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();

        long epoch = registry.register("acme", "click_uz_main", channel);
        ChannelRegistry.ActiveSession session = registry.lookup("acme", "click_uz_main");

        assertEquals(channel, session.channel());
        assertEquals(epoch, session.sessionEpoch());
    }

    @Test
    void lookupReturnsNullWhenNotRegistered() {
        ChannelRegistry registry = new ChannelRegistry();
        assertNull(registry.lookup("acme", "no_such_system"));
    }

    @Test
    void reconnectAssignsNewEpoch() {
        ChannelRegistry registry = new ChannelRegistry();
        long epoch1 = registry.register("acme", "click_uz_main", new EmbeddedChannel());
        long epoch2 = registry.register("acme", "click_uz_main", new EmbeddedChannel());
        assertNotEquals(epoch1, epoch2, "переподключение должно давать новый session_epoch (HLD §11.1)");
    }

    @Test
    void unregisterIfSameChannelDoesNotRemoveNewerSession() {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel oldChannel = new EmbeddedChannel();
        registry.register("acme", "click_uz_main", oldChannel);

        EmbeddedChannel newChannel = new EmbeddedChannel();
        registry.register("acme", "click_uz_main", newChannel);

        // Старая сессия (уже вытесненная) пытается снять регистрацию по unbind —
        // не должна затронуть уже зарегистрированную новую сессию.
        registry.unregisterIfSameChannel("acme", "click_uz_main", oldChannel);

        assertEquals(newChannel, registry.lookup("acme", "click_uz_main").channel());
    }

    @Test
    void unregisterIfSameChannelRemovesMatchingSession() {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel channel = new EmbeddedChannel();
        registry.register("acme", "click_uz_main", channel);

        registry.unregisterIfSameChannel("acme", "click_uz_main", channel);

        assertNull(registry.lookup("acme", "click_uz_main"));
    }
}