package sqlproxy.protocol

import com.google.protobuf.MessageLite
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import sqlproxy.proto.Common
import sqlproxy.proto.ResponseOuterClass.ColumnResponse
import sqlproxy.proto.ResponseOuterClass.QueryResponse
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.Status
import java.sql.ResultSetMetaData

fun newResponseBuilder(id: String, sessionId: String?, stmtId: String?): Response.Builder {
    val meta = Common.Meta.newBuilder().setId(id).also {
        sessionId?.run { it.setSession(sessionId) }
    }.also {
        stmtId?.run { it.setStmt(stmtId) }
    }.build()
    return Response.newBuilder().setMeta(meta)
}

fun newSuccessResponseBuilder(id: String,
                              sessionId: String? = null,
                              stmtId: String? = null): Response.Builder {
    val status = Status.newBuilder().setCode(Status.StatusCode.SUCCESS).build()
    return newResponseBuilder(id, sessionId, stmtId).setStatus(status)
}

fun newFailResponseBuilder(
        id: String,
        sessionId: String? = null,
        stmtId: String? = null,
        msg: String? = null
): Response.Builder {
    val status = Status.newBuilder()
            .setCode(Status.StatusCode.FAILED)
            .also { msg?.run { it.setMessage(msg) } }
            .build()

    return newResponseBuilder (id, sessionId, stmtId).setStatus(status)
}


fun makeQueryResponse(meta: ResultSetMetaData): QueryResponse {
    val columnCount = meta.columnCount
    val queryBuilder = QueryResponse.newBuilder().setNumOfColumn(columnCount)
    for (i in 1 until columnCount) {
        val column = ColumnResponse.newBuilder()
                .setPos(i)
                .setName(meta.getColumnName(i))
                .setType(meta.getColumnType(i))
                .build()
        queryBuilder.addColumnMeta(column)
    }
    return queryBuilder.build()
}



interface ResponseHolder {
    fun write(writeFun: (Any) -> Unit)
}


class FlowResponseHolder(private val flow: Flow<MessageLite>) : ResponseHolder {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    @InternalCoroutinesApi
    override fun write(writeFun: (Any) -> Unit) {
        val job = GlobalScope.launch(Dispatchers.Unconfined) {
            logger.trace { "thread [${Thread.currentThread().name}]" }
            logger.trace { "begin to collect msg" }
            flow.collect { msg -> writeFun(msg) }
            logger.trace { "collect msg done" }
        }
        runBlocking { job.join() }
    }
}
