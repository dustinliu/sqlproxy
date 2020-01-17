package sqlproxy.server

import com.google.protobuf.MessageLite
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass.RowResponse
import sqlproxy.proto.ResponseOuterClass.StmtResponse
import sqlproxy.proto.ResponseOuterClass.UpdateResponse
import sqlproxy.protocol.ProxyRequest
import java.sql.ResultSet

typealias ProtobufAny = com.google.protobuf.Any

object ServiceProvider {
    fun getService(event: Request.Event) = when (event) {
        Request.Event.CONNECT -> { ConnectService() }
        Request.Event.CLOSE -> { CloseService() }
        Request.Event.CREATE_STMT -> { CreateStmtService() }
        Request.Event.SQL_UPDATE -> { SQLUpdateService() }
        Request.Event.SQL_QUERY -> { SQLQueryService() }
        else -> throw IllegalArgumentException("unknown event")
    }
}

interface Service {
    fun handleRequest(request: ProxyRequest): ResponseHolder
}

abstract class AbstractFlowService : Service {
    override fun handleRequest(request: ProxyRequest): ResponseHolder =
        try {
            val flow = processRequest(request)
            FlowResponseHolder(flow)
        } catch (e: RuntimeException) {
            val flow = flow {
                emit(newFailResponseBuilder(
                        request.requestId, request.sessionId, msg = e.message).build()
                )
            }
            FlowResponseHolder(flow)
        }

    abstract fun processRequest(request: ProxyRequest): Flow<MessageLite>
}

class ConnectService : AbstractFlowService() {
    override fun processRequest(request: Request) = flow {
        val sessionId = SessionFactory.newSession().id
        emit(newSuccessResponseBuilder(request.meta.id, sessionId).build())
    }
}

class CloseService : AbstractFlowService() {
    override fun processRequest(request: Request) = flow {
        SessionFactory.closeSession(request.meta.session)
        emit(newSuccessResponseBuilder(request.meta.id, request.meta.session).build())
    }
}

class CreateStmtService : AbstractFlowService() {
    override fun processRequest(request: Request) = flow {
        val session = SessionFactory.getSession(request.meta.session)
        val stmtResponse = StmtResponse.newBuilder().setId(session.createStmt()).build()
        val builder = newSuccessResponseBuilder(request.meta.id, request.meta.session)
        emit(builder.setStmt(stmtResponse).build())
    }
}

class SQLUpdateService : AbstractFlowService() {
    override fun processRequest(request: Request) = flow {
        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.sqlRequest.stmt)
        val count = stmt.executeUpdate(request.sqlRequest.sql)
        val builder = newSuccessResponseBuilder(
                request.meta.id, request.meta.session)
        val updateResponse = UpdateResponse.newBuilder()
                .setStmt(request.sqlRequest.stmt)
                .setUpdateCount(count).build()
        emit(builder.setUpdateResponse(updateResponse).build())
    }
}

abstract class AbstractQueryService : AbstractFlowService() {
    override fun processRequest(request: Request): Flow<MessageLite> = flow {
        val resultSet = getResultSet(request)
        val resultMeta = resultSet.metaData
        val builder = newSuccessResponseBuilder(
                request.meta.id, request.meta.session)
        builder.queryResponse = makeQueryResponse(resultMeta)
        emit(builder.build())

        val meta = Meta.newBuilder().setId(request.meta.id).setSession(request.meta.session)
        while (resultSet.next()) {
            emit(makeRowResponseBuilder(resultSet).setMeta(meta).setHasData(true).build())
        }
        emit(RowResponse.newBuilder().setMeta(meta).setHasData(false).build())
    }

    abstract fun getResultSet(request: Request): ResultSet
}

class SQLQueryService : AbstractQueryService() {
    override fun getResultSet(request: Request): ResultSet {
        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.sqlRequest.stmt)
        return stmt.executeQuery(request.sqlRequest.sql)
    }
}
