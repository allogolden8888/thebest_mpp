package uz.mpp.partnersmpp.server;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void sessionsForPartnerReturnsAllBindsOfThatPartnerOnly() {
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel acmeMain = new EmbeddedChannel();
        EmbeddedChannel acmeAlt = new EmbeddedChannel();
        EmbeddedChannel betaMain = new EmbeddedChannel();
        registry.register("acme", "click_uz_main", acmeMain);
        registry.register("acme", "click_uz_alt", acmeAlt);
        registry.register("beta", "beta_main", betaMain);

        List<ChannelRegistry.PartnerSession> acmeSessions = registry.sessionsForPartner("acme");

        assertEquals(2, acmeSessions.size());
        assertTrue(acmeSessions.stream().anyMatch(s -> s.systemId().equals("click_uz_main") && s.session().channel() == acmeMain));
        assertTrue(acmeSessions.stream().anyMatch(s -> s.systemId().equals("click_uz_alt") && s.session().channel() == acmeAlt));
        assertTrue(acmeSessions.stream().noneMatch(s -> s.session().channel() == betaMain), "чужой партнёр не должен попадать в выборку");
    }

    @Test
    void sessionsForPartnerReturnsEmptyListWhenNoneBound() {
        ChannelRegistry registry = new ChannelRegistry();
        assertTrue(registry.sessionsForPartner("nobody").isEmpty());
    }

    @Test
    void sessionsForPartnerDoesNotPrefixMatchDifferentPartnerId() {
        // "acme" не должен по ошибке подобрать сессии "acme2" — startsWith
        // без явной проверки границы ключа схлопнул бы их (id вложены как
        // подстрока), см. реализацию (сначала startsWith, затем точное
        // сравнение partnerIdOf(key)).
        ChannelRegistry registry = new ChannelRegistry();
        EmbeddedChannel acme2Channel = new EmbeddedChannel();
        registry.register("acme2", "some_system", acme2Channel);

        assertTrue(registry.sessionsForPartner("acme").isEmpty());
    }
}