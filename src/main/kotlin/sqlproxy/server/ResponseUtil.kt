package sqlproxy.server

import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import com.google.protobuf.StringValue
import com.google.protobuf.UInt32Value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import sqlproxy.proto.Common
import sqlproxy.proto.ResponseOuterClass
import sqlproxy.proto.ResponseOuterClass.ColumnResponse
import sqlproxy.proto.ResponseOuterClass.QueryResponse
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.RowResponse
import sqlproxy.proto.ResponseOuterClass.Status
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData

fun newResponseBuilder(id: String, sessionId: Long): Response.Builder {
    val meta = Common.Meta.newBuilder().setId(id).setSession(sessionId).build()
    return Response.newBuilder().setMeta(meta)
}

fun newSuccessResponseBuilder(id: String,
                              sessionId: Long) : Response.Builder {
    val status = Status.newBuilder().setCode(Status.StatusCode.SUCCESS).build()
    return newResponseBuilder(id, sessionId).setStatus(status)
}

fun newFailResponseBuilder(
        id: String,
        sessionId: Long,
        msg: String? = null
): Response.Builder {
    val status = Status.newBuilder()
            .setCode(Status.StatusCode.FAILED)
            .also { msg?.run { it.setMessage(msg) } }
            .build()

    return newResponseBuilder(id, sessionId).setStatus(status)
}

fun makeQueryResponse(meta: ResultSetMetaData): QueryResponse {
    val columnCount = meta.columnCount
    val queryBuilder = QueryResponse.newBuilder().setNumOfColumn(columnCount)
    for (i in 1 until columnCount+1) {
        val column = ColumnResponse.newBuilder()
                .setPos(i)
                .setName(meta.getColumnName(i))
                .setType(meta.getColumnType(i))
                .build()
        queryBuilder.addColumnMeta(column)
    }
    return queryBuilder.build()
}

fun makeRowResponseBuilder(resultSet: ResultSet): RowResponse.Builder {
    val rowBuilder = ResponseOuterClass.RowResponse.newBuilder()
    val resultMeta = resultSet.metaData
    for (i in 1 until resultMeta.columnCount + 1) {
        val value: Message = when (val type = JDBCType.valueOf(resultMeta.getColumnType(i))) {
            JDBCType.CHAR, JDBCType.VARCHAR -> StringValue.of(resultSet.getString(i))
            JDBCType.INTEGER -> UInt32Value.of(resultSet.getInt(i))
            else -> throw IllegalArgumentException(
                    "unsupport value type, [${type.getName()}]")
        }
        val column = ColumnResponse.newBuilder().setPos(i).setValue(ProtobufAny.pack(value))
        rowBuilder.addColumn(column)
    }
    return rowBuilder
}

interface ResponseHolder {
    fun write(writeFun: (Any) -> Unit)
}

class FlowResponseHolder(private val flow: Flow<MessageLite>) : ResponseHolder {
    @InternalCoroutinesApi
    override fun write(writeFun: (Any) -> Unit) {
        val job = GlobalScope.launch(Dispatchers.Unconfined) {
            flow.collect { msg -> writeFun(msg) }
        }
        runBlocking { job.join() }
    }
}
