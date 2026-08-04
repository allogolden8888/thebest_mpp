package uz.mpp.billingledgerwriter.store;

import org.jooq.ConnectionProvider;
import org.jooq.exception.DataAccessException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Закрывает часть CRITICAL находки кодревью (CODE_REVIEW.md,
 * "billing-ledger-writer" #3): раньше единственный {@link Connection}
 * создавался один раз в {@code Main.main} и передавался в {@code DSL.using(connection, ...)}
 * НАПРЯМУЮ — если Postgres TCP-соединение рвётся по любой причине, ЭТОТ ЖЕ
 * сломанный объект переиспользовался НАВСЕГДА, и КАЖДАЯ последующая запись
 * падала одинаково, бесконечно (реальный сценарий: failover Postgres на
 * 30 секунд — окно потерь становится неограниченным, не 30-секундным).
 *
 * <p>jOOQ {@link ConnectionProvider} вызывается перед КАЖДЫМ выполнением
 * statement'а (для нашего паттерна "один insert за вызов" — практически
 * перед каждой записью) — {@link #acquire()} проверяет {@code isValid} и
 * прозрачно переподключается, если соединение мертво, вместо того чтобы
 * молча отдавать тот же нерабочий объект. Не полноценный pool (осознанное
 * упрощение, как и раньше — один долгоживущий connection, не HikariCP) —
 * просто ЖИВОЙ один connection вместо МЁРТВОГО одного connection.
 */
public final class ReconnectingConnectionProvider implements ConnectionProvider {

    private final String jdbcUrl;
    private volatile Connection connection;

    public ReconnectingConnectionProvider(String jdbcUrl) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.connection = DriverManager.getConnection(jdbcUrl);
    }

    @Override
    public Connection acquire() throws DataAccessException {
        try {
            if (connection == null || !connection.isValid(2)) {
                closeQuietly();
                connection = DriverManager.getConnection(jdbcUrl);
            }
            return connection;
        } catch (SQLException e) {
            throw new DataAccessException("не удалось (пере)подключиться к Postgres: " + e.getMessage(), e);
        }
    }

    @Override
    public void release(Connection connection) {
        // Долгоживущее единственное соединение, намеренно не закрывается
        // между statement'ами — жизненным циклом управляет acquire() выше
        // (переподключение только когда реально сломано) и close() ниже
        // (shutdown).
    }

    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // при закрытии уже сломанного соединения ошибка не важна
            }
        }
    }
}
