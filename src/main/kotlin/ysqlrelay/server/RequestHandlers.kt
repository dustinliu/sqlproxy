package ysqlrelay.server

import ysqlrelay.proto.Request.SqlRequest.Event
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import ysqlrelay.proto.Request.SqlRequest as RelayRequest
import ysqlrelay.proto.Response.SqlResponse as RelayResponse


fun handleRequest(request: RelayRequest): RelayResponse {
    when(request.cmd) {
        Event.CONNECT -> {
            SessionFactory.newSession()
        }

        Event.CLOSE -> {
            SessionFactory.getSession(request.session.id)
        }

        else -> throw RuntimeException("unknown request")
    }
}


class Session {
    val id = UUID.randomUUID().toString()
    val created = System.currentTimeMillis()

    fun close() {}
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
}


class ConnectHandler {
    fun handleRequest(request: RelayRequest): RelayResponse {
        val builder = getResponseBuilder().setId(request.id)
    }
}
