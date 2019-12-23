package sqlproxy.server

import sqlproxy.grpc.Sqlproxy
import sqlproxy.protocol.getMeta
import sqlproxy.protocol.getStatus
import java.util.*
import java.util.concurrent.ConcurrentHashMap


class Session {
    val id = UUID.randomUUID().toString()
    val created = System.currentTimeMillis()
    private val connection = DataSource.getConnection()

    fun close() {
        connection.close()
    }
}


object SessionFactory {
    private val session_pool = ConcurrentHashMap<String, Session>()

    fun newSession() = Session().also { session_pool[it.id] = it }

    fun getSession(session_id: String) = session_pool.getValue(session_id)

    fun closeSession(session: Session) {
        session.close()
        session_pool.remove(session.id)
    }

    fun getSessions(): MutableCollection<Session> {
        println("pool size: ${session_pool.size}")
        return session_pool.values
    }
}


class ConnectHandler {
    fun handleRequest(request: Sqlproxy.ConnectRequest): Sqlproxy.ConnectResponse {
        val session = SessionFactory.newSession()
        return Sqlproxy.ConnectResponse.newBuilder()
            .setMeta(getMeta(request.meta.id, session.id))
            .setStatus(getStatus(Sqlproxy.Status.StatusCode.SUCCESS))
            .build()
    }
}

class CloseHandler {
    fun handleRequest(request: Sqlproxy.CloseRequest): Sqlproxy.CloseResponse {
        val session = SessionFactory.getSession(request.meta.session)
        SessionFactory.closeSession(session)
        return Sqlproxy.CloseResponse.newBuilder()
            .setMeta(getMeta(request.meta.id, session.id))
            .setStatus(getStatus(Sqlproxy.Status.StatusCode.SUCCESS))
            .build()
    }
}
