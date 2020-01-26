package sqlproxy.server

import kotlinx.coroutines.flow.flow
import sqlproxy.proto.Common.Statement.Type.STATEMENT
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.protocol.FlowResponseHolder
import sqlproxy.protocol.ProxyColumnResponse
import sqlproxy.protocol.ProxyQueryResponse
import sqlproxy.protocol.ProxyRequest
import sqlproxy.protocol.ProxyResponse
import sqlproxy.protocol.ProxySQLRequest
import sqlproxy.protocol.ProxyStatement
import sqlproxy.protocol.ProxyStmtResponse
import sqlproxy.protocol.ProxyUpdateResponse
import sqlproxy.protocol.ResponseHolder
import java.sql.ResultSet
import java.sql.SQLException

typealias ProtobufAny = com.google.protobuf.Any

object ServiceProvider {
    fun getService(event: Request.Event) = when (event) {
        Request.Event.CONNECT -> ConnectService()
        Request.Event.CLOSE -> CloseService()
        Request.Event.CREATE_STMT -> CreateStmtService()
        Request.Event.SQL_UPDATE -> SQLUpdateService()
        Request.Event.SQL_QUERY -> { SQLQueryService() }
        else -> throw IllegalArgumentException("unknown event")
    }
}

interface Service {
    fun handleRequest(request: ProxyRequest): ResponseHolder
}

abstract class AbstractFlowService : Service {
    override fun handleRequest(request: ProxyRequest): ResponseHolder {
        val flow = try {
            flow { emit(processRequest(request)) }
        } catch (e: SQLException) {
            flow { emit(ProxyResponse.failedResponse(request, e.errorCode, e.message)) }
        }
        return FlowResponseHolder(flow)
    }

    abstract fun processRequest(request: ProxyRequest): ProxyResponse
}

class ConnectService : AbstractFlowService() {
    override fun processRequest(request: ProxyRequest): ProxyResponse {
        val sessionId = SessionFactory.newSession().id
        return ProxyResponse.successResponse(request, sessionId = sessionId)
    }
}

class CloseService : AbstractFlowService() {
    override fun processRequest(request: ProxyRequest): ProxyResponse {
        SessionFactory.closeSession(request.sessionId)
        return ProxyResponse.successResponse(request)
    }
}

class CreateStmtService : AbstractFlowService() {
    override fun processRequest(request: ProxyRequest): ProxyResponse {
        val session = SessionFactory.getSession(request.sessionId)
        val stmtResponse =
                ProxyStmtResponse(ProxyStatement(session.createStmt(), STATEMENT))
        return ProxyResponse.successResponse(request, stmtResponse)
    }
}

class SQLUpdateService : AbstractFlowService() {
    override fun processRequest(request: ProxyRequest): ProxyResponse {
        val sqlRequest = request.getSubRequestByType<ProxySQLRequest>()
        val stmt = SessionFactory.getSession(request.sessionId).getStmt(sqlRequest.statement.id)
        val count = stmt.executeUpdate(sqlRequest.sql)
        val updateResponse =
                ProxyUpdateResponse(sqlRequest.statement, count)
        return ProxyResponse.successResponse(request, updateResponse)
    }
}

abstract class AbstractQueryService : AbstractFlowService() {
    override fun processRequest(request: ProxyRequest): ProxyResponse {
        val sqlRequest = request.getSubRequestByType<ProxySQLRequest>()
        val resultSet = getResultSet(request)
        val queryResponse =  ProxyQueryResponse(
                sqlRequest.statement,
                resultSet.metaData.columnCount,
                makeColumnResponse(resultSet))
        return ProxyResponse.successResponse(request, queryResponse)
    }

    private fun makeColumnResponse(resultSet: ResultSet): List<ProxyColumnResponse> {
        val meta = resultSet.metaData
        val metas = mutableListOf<ProxyColumnResponse>()
        for (i in 1 until meta.columnCount+1) {
            metas.add(ProxyColumnResponse(i, meta.getColumnName(i), meta.getColumnType(i)))
        }
        return metas
    }

    abstract fun getResultSet(request: ProxyRequest): ResultSet
}

class SQLQueryService : AbstractQueryService() {
    override fun getResultSet(request: ProxyRequest): ResultSet {
        val sqlRequest = request.getSubRequestByType<ProxySQLRequest>()
        val stmt = SessionFactory.getSession(request.sessionId).getStmt(sqlRequest.stmtId)
        return stmt.executeQuery(sqlRequest.sql)
    }
}
