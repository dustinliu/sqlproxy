package sqlproxy.server

import mu.KotlinLogging
import sqlproxy.proto.Request.SqlRequest.Event
import ysqlrelay.proto.Common
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import sqlproxy.proto.Request.SqlRequest as RelayRequest
import sqlproxy.proto.Response.SqlResponse as RelayResponse

fun handleRequest(request: RelayRequest): RelayResponse {
    val handler = when(request.event) {
        Event.CONNECT -> { ConnectHandler() }

        Event.CLOSE -> { CloseHandler() }

        else -> throw RuntimeException("unknown request")
    }
    return handler.handleRequest(request)
}


class Session {
    enum class Status(val code: Int) {
        CONNECTED(1),
        CLOSED(2)
    }

    var status: Status = Status.CONNECTED
    val id = UUID.randomUUID().toString()
    val created = System.currentTimeMillis()

    fun close() {
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
}

interface RequestHandler {
    fun handleRequest(request: RelayRequest): RelayResponse
}

class ConnectHandler: RequestHandler {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun handleRequest(request: RelayRequest): RelayResponse {
        logger.trace {"req id: ${request.meta.id}"}
        logger.trace {"req session: ${request.meta.session}"}

        val meta = Common.Meta.newBuilder()
                .setId(request.meta.id)
                .setSession(SessionFactory.newSession().id)
        return RelayResponse.newBuilder()
                .setMeta(meta)
                .setStatus(Common.Status.newBuilder().setCode(Common.Status.StatusCode.SUCCESS))
                .build()
    }
}

class CloseHandler: RequestHandler {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun handleRequest(request: RelayRequest): RelayResponse {
        logger.trace {"req id: ${request.meta.id}"}
        logger.trace {"req session: ${request.meta.session}"}

        val meta = Common.Meta.newBuilder()
            .setId(request.meta.id)
            .setSession(request.meta.session)
        SessionFactory.closeSession(request.meta.session)
        return RelayResponse.newBuilder()
            .setMeta(meta)
            .setStatus(Common.Status.newBuilder().setCode(Common.Status.StatusCode.SUCCESS))
            .build()
    }
}
