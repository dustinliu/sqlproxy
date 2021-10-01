package sqlproxy.server.service

import mu.KotlinLogging
import java.sql.Connection
import java.sql.SQLException

private val logger = KotlinLogging.logger {}

class Session: AutoCloseable {
    val connection: Connection = DataSource.getConnection()
    var active = false

    init {
        active = true
    }

    override fun close() {
        try {
            connection.close()
        } catch (e: SQLException) {
            logger.warn {"Error(${e.errorCode}): ${e.message}"}
        } finally {
            active = false
        }
    }


    fun isActive() = active
}
