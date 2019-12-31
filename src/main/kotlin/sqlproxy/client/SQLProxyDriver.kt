package sqlproxy.client

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.util.*
import java.util.logging.Logger


class SQLProxyDriver: Driver {
    companion object {
        val prefix = "jdbc:sqlproxy:"
    }

    override fun connect(url: String, info: Properties?): Connection {
        return SQLProxyConnection(url)
    }

    override fun getMinorVersion(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getParentLogger(): Logger {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun jdbcCompliant(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun acceptsURL(url: String): Boolean {
        return url.startsWith(prefix)
    }

    override fun getMajorVersion(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
