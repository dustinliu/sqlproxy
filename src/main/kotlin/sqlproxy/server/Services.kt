package sqlproxy.server

import mu.KotlinLogging
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass
import java.lang.Exception


object ServiceProvider {
    fun getService(event: Request.Event) = when(event) {
        Request.Event.CONNECT -> { ConnectService() }
        Request.Event.CLOSE -> { CloseService() }
//        Request.Event.CREATE_STMT -> { CreateStmtService() }
        else -> throw RuntimeException("unknown event")
    }
}

abstract class Service {
    companion object { private val logger = KotlinLogging.logger {} }

    fun handleRequest(request: Request) = try {
        logger.trace {"req id: ${request.meta.id}"}
        logger.trace {"req session: ${request.meta.session}"}

        processRequest(request)
    } catch (e: Exception) {
        ResponseHolder.failedResponse(request.meta.id, request.meta.session, e.message)
    }

    abstract fun processRequest(request: Request): ResponseHolder
}


class ConnectService: Service() {
    override fun processRequest(request: Request): ResponseHolder {
        return ResponseHolder.successBuilder(request.meta.id, SessionFactory.newSession().id)
    }
}

class CloseService: Service() {
    override fun processRequest(request: Request): ResponseHolder {
        SessionFactory.closeSession(request.meta.session)
        return ResponseHolder.successBuilder(request.meta.id, request.meta.session)
    }
}

//class CreateStmtService : Service() {
//    companion object { private val logger = KotlinLogging.logger {} }
//
//    override fun processRequest(request: Request): ResponseHolder {
//        val session = SessionFactory.getSession(request.meta.session)
//
//        val stmt = ResponseOuterClass.StmtResponse.newBuilder().setId(session.createStmt())
//    }
//}
