package sqlproxy.server

import java.sql.Connection
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class Session {
    enum class Status {
        CONNECTED,
        CLOSED
    }

    companion object {
        private val atomicId = AtomicLong(1)
        private fun generateId() = atomicId.getAndIncrement()
    }

    private val stmtIdHolder: AtomicInteger = AtomicInteger(1)
    var status: Status = Status.CONNECTED
    val id = generateId()
    val created = System.currentTimeMillis()

    private val connection: Connection = DataSourceFactory.dataSource.connection
    private val stmtMap = mutableMapOf<Int, Statement>()

    fun createStmt(): Int {
        val id = stmtIdHolder.getAndIncrement()
        stmtMap[id] = connection.createStatement()
        return id
    }

    fun getStmt(id: Int) = stmtMap.getValue(id)

    fun close() {
        stmtMap.forEach { (_, stmt) -> stmt.close() }
        connection.close()
        status = Status.CLOSED
    }
}

object SessionFactory {
    private val session_pool = ConcurrentHashMap<Long, Session>()

    fun newSession() = Session().also { session_pool[it.id] = it }

    fun getSession(sessionId: Long) = session_pool.getValue(sessionId)

    fun closeSession(sessionId: Long) {
        val session = getSession(sessionId)
        session.close()
        session_pool.remove(sessionId)
    }

    fun resetAll() {
        for ((k, v) in session_pool) {
            v.close()
        }
        session_pool.clear()
    }

    internal fun getSessions() = session_pool
}
