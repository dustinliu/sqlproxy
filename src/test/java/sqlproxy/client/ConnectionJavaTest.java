package sqlproxy.client;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sqlproxy.server.EmbeddedServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionJavaTest extends EmbeddedServer {
    @BeforeAll
    private static void initDriver() throws SQLException {
        DriverManager.registerDriver(new SQLProxyDriver());
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlproxy://localhost:8888");
    }

    @Test
    void newConnection() throws SQLException {
        var conn = getConnection();
        conn.close();
    }

    @Test
    void isValid() throws SQLException {
        var conn = getConnection();
        assertTrue(conn.isValid(5));
    }
}
