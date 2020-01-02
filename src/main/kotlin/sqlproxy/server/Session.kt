package sqlproxy.server

import java.sql.Statement
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class Session {
    enum class Status {
        CONNECTED,
        CLOSED
    }


    var status: Status = Status.CONNECTED
    val id = UUID.randomUUID().toString()
    val created = System.currentTimeMillis()

    private val connection = DataSource.getConnection()
    private val stmtMap = mutableMapOf<String, Statement>()

    fun createStmt(): String {
        val id = UUID.randomUUID().toString()
        stmtMap[id] = connection.createStatement()
        return id
    }

    fun getStmt(id: String) = stmtMap.getValue(id)

    fun close() {
        stmtMap.forEach { (_, stmt) -> stmt.close() }
        connection.close()
        status = Status.CLOSED
    }
}


object SessionFactory {
    private val session_pool = ConcurrentHashMap<String, Session>()

    fun newSession() = Session().also { session_pool[it.id] = it }

    fun getSession(session_id: String) = session_pool.getValue(session_id)

    fun closeSession(session_id: String) {
        val session = getSession(session_id)
        session.close()
        session_pool.remove(session_id)
    }

    fun resetAll() {
        for ((k, v) in session_pool) {
            v.close()
        }
        session_pool.clear()
    }

    internal fun getSessions() = session_pool
}
