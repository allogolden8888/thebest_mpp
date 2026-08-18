package uz.mpp.partnersmpp.vault;

import java.io.IOException;

/**
 * Узкий интерфейс, который {@code
 * uz.mpp.partnersmpp.server.VaultAuthenticator} реально зависит —
 * {@link VaultClient} его реализует (реальный HTTP поверх Vault KV v2),
 * тесты могут подставить фейк без реальной сети для случаев, которые
 * требуют детерминированного контроля (кеш-хит не должен звать сеть
 * второй раз, TTL истёк -> звать снова, недоступность Vault -> fail
 * closed) — round-trip против реального {@code vault server -dev} живёт
 * отдельно, в тестах самого {@link VaultClient}.
 */
public interface VaultSecretReader {
    String readProperty(String kvPath, String property) throws IOException, InterruptedException;
}
