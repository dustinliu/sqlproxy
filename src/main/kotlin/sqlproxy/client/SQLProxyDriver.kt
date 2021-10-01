package sqlproxy.client

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.util.*
import java.util.logging.Logger

class SQLProxyDriver : Driver {
    companion object {
        internal const val prefix = "jdbc:sqlproxy:"
    }

    override fun connect(url: String?, info: Properties?): Connection {
        if (!acceptsURL(url)) throw IllegalArgumentException("invalid jdbc url: $url")
        return SQLProxyConnection(url!!, info)
    }

    override fun acceptsURL(url: String?) = url != null && url.startsWith(prefix)

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        TODO("Not yet implemented")
    }

    override fun getMajorVersion() = 1

    override fun getMinorVersion() = 2

    override fun jdbcCompliant() = false

    override fun getParentLogger(): Logger {
        TODO("Not yet implemented")
    }
}
