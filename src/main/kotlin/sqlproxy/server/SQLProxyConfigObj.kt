package sqlproxy.server

import com.sksamuel.hoplite.ConfigLoader
import java.time.Duration

val SQLProxyConfig = ConfigLoader().loadConfigOrThrow<SQLProxyConfigObj>("/sqlproxy.yml")

data class SQLProxyConfigObj(val env: String, val server: Server, val database: Database) {
    data class Server(val address: String, val port: Int, val numOfServiceThread: Int)
    data class Database(val jdbcUrl: String, val connection: Connection)
    data class Connection(val maxLifetime: Duration)
}
