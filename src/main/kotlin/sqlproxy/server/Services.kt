package sqlproxy.server

import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import com.google.protobuf.StringValue
import com.google.protobuf.UInt32Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass
import sqlproxy.proto.ResponseOuterClass.ColumnResponse
import sqlproxy.proto.ResponseOuterClass.RowResponse
import sqlproxy.proto.ResponseOuterClass.StmtResponse
import java.sql.JDBCType
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
    fun handleRequest(request: Request): ResponseHolder
}

abstract class AbstractFlowService : Service {
    override fun handleRequest(request: Request): ResponseHolder =
        try {
            val flow = processRequest(request)
            FlowResponseHolder(flow)
        } catch (e: RuntimeException) {
            val flow = flow {
                emit(newFailResponseBuilder(
                        request.meta.id, request.meta.session, msg = e.message).build()
                )
            }
            FlowResponseHolder(flow)
        }

    abstract fun processRequest(request: Request): Flow<MessageLite>
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
        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.meta.stmt)
        val count = stmt.executeUpdate(request.sql)
        val builder = newSuccessResponseBuilder(
                request.meta.id, request.meta.session, request.meta.stmt)
        emit(builder.setUpdateCount(count).build())
    }
}

class SQLQueryService : AbstractFlowService() {
    override fun processRequest(request: Request): Flow<MessageLite> = flow {
        val stmt = SessionFactory.getSession(request.meta.session).getStmt(request.meta.stmt)
        val resultSet = stmt.executeQuery(request.sql)
        val resultMeta = resultSet.metaData
        val builder = newSuccessResponseBuilder(
                request.meta.id, request.meta.session, request.meta.stmt)
        builder.queryResponse = makeQueryResponse(resultMeta)
        emit(builder.build())

        val meta = Meta.newBuilder().setId(request.meta.id).setSession(request.meta.session)
        while (resultSet.next()) {
            emit(makeRowResponseBuilder(resultSet).setMeta(meta).setHasData(true).build())
        }
        emit(RowResponse.newBuilder().setMeta(meta).setHasData(false).build())
    }
}
