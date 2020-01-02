package sqlproxy.server

import com.google.protobuf.MessageLite
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass.StmtResponse
import sqlproxy.protocol.ResponseHolder
import sqlproxy.protocol.newFailResponseBuilder
import sqlproxy.protocol.newSuccessResponseBuilder


object ServiceProvider {
    fun getService(event: Request.Event) = when(event) {
        Request.Event.CONNECT -> { ConnectService() }
        Request.Event.CLOSE -> { CloseService() }
        Request.Event.CREATE_STMT -> { CreateStmtService() }
        Request.Event.SQL_UPDATE -> { SQLUpdateService() }
        else -> throw RuntimeException("unknown event")
    }
}


interface Service {
    fun handleRequest(request: Request): ResponseHolder
}


abstract class AbstractChannelService: Service {
    companion object { private val logger = KotlinLogging.logger {} }
    val channel = Channel<MessageLite>(Channel.BUFFERED)

    override fun handleRequest(request: Request): ResponseHolder {
        logger.trace { "running in thread [${Thread.currentThread().name}]"}
        logger.trace { "req id: ${request.meta.id}" }
        logger.trace { "req session: ${request.meta.session}" }
        logger.trace { "req event: ${request.event}"}

        logger.trace { "begine processRequest"}
        GlobalScope.launch {
//        withContext() {
            logger.trace { "running in thread [${Thread.currentThread().name}]"}
            try {
                processRequest(request)
            } catch (e: Exception) {
                channel.send(newFailResponseBuilder(request.meta.id,
                        request.meta.session, request.meta.stmt, e.message).build())
            } finally {
                channel.close()
                logger.trace { "coroutine channel closed" }
            }
            logger.trace { "processRequest done" }
        }
        logger.trace { "creating ResponseHolder" }
        return ResponseHolder(channel)
    }

//    protected suspend fun writeResponse(msg: MessageLite) {
//        logger.trace { "running in thread [${Thread.currentThread().name}]"}
//        logger.trace {"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}
//        channel.send(msg)
//        logger.trace { "msg sent to channel" }
//    }

    abstract suspend fun processRequest(request: Request)
}


class ConnectService: AbstractChannelService() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }
    override suspend fun processRequest(request: Request) {
        logger.trace { "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" }
        val sessionId = SessionFactory.newSession().id
        logger.trace { "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" }
        channel.send(newSuccessResponseBuilder(request.meta.id, sessionId).build())
        logger.debug { "xxxxxxxxxxxxxxxxxx msg sent to channel" }
    }
}


class CloseService: AbstractChannelService() {
    override suspend fun processRequest(request: Request) {
        SessionFactory.closeSession(request.meta.session)
        channel.send(newSuccessResponseBuilder(request.meta.id, request.meta.session).build())
    }
}


class CreateStmtService: AbstractChannelService() {
    override suspend fun processRequest(request: Request) {
        val session = SessionFactory.getSession(request.meta.session)
        val stmtResponse = StmtResponse.newBuilder().setId(session.createStmt()).build()
        val builder = newSuccessResponseBuilder(request.meta.id, request.meta.session)
        channel.send(builder.setStmt(stmtResponse).build())
    }
}

class SQLUpdateService: AbstractChannelService() {
    override suspend fun processRequest(request: Request) {
        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.meta.stmt)
        val count = stmt.executeUpdate(request.sql)
        val builder = newSuccessResponseBuilder(request.meta.id, request.meta.session, request.meta.stmt)
        channel.send(builder.setUpdateCount(count).build())
    }
}

//class SQLQueryService: AbstractChannelService() {
//    override suspend fun processRequest(request: Request) {
//        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.meta.stmt)
//        val resultSet = stmt.executeQuery(request.sql)
//        val resultMeta = resultSet.metaData
//
//        val builder = newSuccessResponseBuilder(request.meta.id, request.meta.session, request.meta.stmt)
//    }
//}
