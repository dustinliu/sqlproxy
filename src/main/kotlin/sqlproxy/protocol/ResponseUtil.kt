package sqlproxy.protocol

import com.google.protobuf.MessageLite
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import sqlproxy.proto.Common
import sqlproxy.proto.ResponseOuterClass.*
import java.sql.ResultSetMetaData

fun newResponseBuilder(id: String, sessionId: String?, stmtId: String?): Response.Builder {
    val meta = Common.Meta.newBuilder().setId(id).also {
        sessionId?.run { it.setSession(sessionId) }
    }.also {
        stmtId?.run { it.setStmt(stmtId) }
    }.build()
    return Response.newBuilder().setMeta(meta)
}


fun newSuccessResponseBuilder(id: String, sessionId: String?=null, stmtId: String?=null): Response.Builder {
    val status = Status.newBuilder() .setCode(Status.StatusCode.SUCCESS).build()
    return newResponseBuilder(id, sessionId, stmtId).setStatus(status)
}

fun newFailResponseBuilder(id: String, sessionId: String?=null, stmtId: String?=null, msg: String?=null): Response.Builder {
    val status = Status.newBuilder()
            .setCode(Status.StatusCode.FAILED)
            .also { msg?.run { it.setMessage(msg) } }
            .build()
    return newResponseBuilder(id, sessionId, stmtId).setStatus(status)
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

class ResponseHolder(private val channel: Channel<MessageLite>) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    fun write(writeFun: (Any) -> Unit)= runBlocking {
        logger.trace { "begin to write message"}
//        val job = GlobalScope.launch {
        val job = launch {
            logger.trace { "running in [thread ${Thread.currentThread().name}]"}
            for (msg in channel) {
                if (msg is Response) {
                    logger.trace { "msg id: ${msg.meta.id}" }
                    logger.trace { "msg session: ${msg.meta.session}" }
                    logger.trace { "status code: ${msg.status.code}" }
                    logger.trace { "status message: ${msg.status.message}" }
                }
                logger.trace { "write msg, ${msg.javaClass}" }
                writeFun(msg)
            }
        }
        job.join()
        logger.trace { "all message written"}
    }
}
