package sqlproxy.protocol

import com.google.protobuf.BoolValue
import com.google.protobuf.DoubleValue
import com.google.protobuf.FloatValue
import com.google.protobuf.Int32Value
import com.google.protobuf.Int64Value
import com.google.protobuf.Message
import com.google.protobuf.StringValue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.ResponseOuterClass
import sqlproxy.proto.ResponseOuterClass.Response.ResultCase
import java.sql.JDBCType

typealias ProtobufAny = com.google.protobuf.Any

class ResponseStatus(val success: Boolean, val appCode: Int = 0, val message: String? = null)
    : ProxyMessage {
    constructor(status: ResponseOuterClass.Status)
            : this(status.code == ResponseOuterClass.Status.StatusCode.SUCCESS, status.appCode, status.message)

    private val status by lazy {
        val code = if (success) ResponseOuterClass.Status.StatusCode.SUCCESS
                   else ResponseOuterClass.Status.StatusCode.FAILED
        ResponseOuterClass.Status.newBuilder().setCode(code).setAppCode(appCode).apply {
            message?.let { setMessage(message) }
        }.build()
    }
    override fun toProtoBuf(): ResponseOuterClass.Status = status
}

class ProxyResponse(meta: ProxyMetaMessage, private val status: ResponseStatus,
                    val subResponse: SubResponse? = null) : PrimaryMessage by meta {
    private val response by lazy {
        var builder = ResponseOuterClass.Response.newBuilder().setMeta(meta.toProtoBuf()).setStatus(status.toProtoBuf())
        // assign value will enable the compiler to check if all branches applied
        builder = when (subResponse) {
            is ProxyStmtResponse -> builder.setStmtResponse(subResponse.toProtoBuf())
            is ProxyUpdateResponse -> builder.setUpdateResponse(subResponse.toProtoBuf())
            is ProxyQueryResponse -> builder.setQueryResponse(subResponse.toProtoBuf())
            null -> builder
        }
        builder.build()
    }
    fun isSuccessful() = status.success
    fun getErrorCode() = status.appCode
    fun getErrorMessage() = status.message

    inline fun <reified T : SubResponse> getSubResponseByType(): T =
            if (subResponse is T) subResponse
            else throw IllegalArgumentException("sub response is not a instance of ${T::class.java}")

    companion object {
        fun successResponse(request: ProxyRequest, subResponse: SubResponse? = null,
                            sessionId: Long? = null): ProxyResponse {
            val status = ResponseStatus(true)
            val meta = ProxyMetaMessage(request.requestId, sessionId?:request.sessionId)
            return ProxyResponse(meta, status, subResponse)
        }

        fun failedResponse(request: ProxyRequest, appCode: Int, message: String?): ProxyResponse {
            val status = ResponseStatus(false, appCode, message)
            val meta = ProxyMetaMessage(request.requestId, request.sessionId)
            return ProxyResponse(meta, status)
        }

        fun fromProtoBuf(response: ResponseOuterClass.Response): ProxyResponse {
            val subResponse = when (response.resultCase!!) {
                ResultCase.STMTRESPONSE -> ProxyStmtResponse.fromProtoBuf(response.stmtResponse)
                ResultCase.UPDATERESPONSE -> ProxyUpdateResponse.fromProtoBuf(response.updateResponse)
                ResultCase.QUERYRESPONSE -> ProxyQueryResponse.fromProtoBuf(response.queryResponse)
                ResultCase.ROWRESPONSE-> TODO()
                ResultCase.RESULT_NOT_SET -> null
            }
            return ProxyResponse(ProxyMetaMessage(response.meta), ResponseStatus(response.status), subResponse)
        }
    }

    override fun toProtoBuf(): ResponseOuterClass.Response = response
}

sealed class SubResponse: ProxyMessage

class ProxyStmtResponse(private val statement: ProxyStatement) : SubResponse() {
    private val response: ResponseOuterClass.StmtResponse by lazy {
        ResponseOuterClass.StmtResponse.newBuilder().setStmt(statement.toProtoBuf()).build()
    }

    val stmtId = statement.id

    override fun toProtoBuf() = response

    companion object {
        fun fromProtoBuf(response: ResponseOuterClass.StmtResponse) =
                ProxyStmtResponse(ProxyStatement.fromProtoBuf(response.stmt))
    }
}

class ProxyUpdateResponse(private val statement: ProxyStatement, val count: Int) : SubResponse() {
    private val response: ResponseOuterClass.UpdateResponse by lazy {
        ResponseOuterClass.UpdateResponse.newBuilder().setStmt(statement.toProtoBuf()).setUpdateCount(count).build()
    }

    override fun toProtoBuf() = response

    companion object {
        fun fromProtoBuf(response: ResponseOuterClass.UpdateResponse) =
                ProxyUpdateResponse(ProxyStatement(response.stmt.id, response.stmt.type),
                        response.updateCount)
    }
}

class ProxyQueryResponse(statement: ProxyStatement, val numOfColumn: Int,
                         columns: List<ProxyColumnResponse> = listOf()): SubResponse() {
    private val response: ResponseOuterClass.QueryResponse by lazy {
       ResponseOuterClass.QueryResponse.newBuilder().setStmt(statement.toProtoBuf() as Statement)
               .setNumOfColumn(numOfColumn).build()
    }

    override fun toProtoBuf() = response

    companion object {
        fun fromProtoBuf(response: ResponseOuterClass.QueryResponse): ProxyQueryResponse {
            val columns = response.columnMetaList.map { ProxyColumnResponse.fromProtoBuf(it) }
            return ProxyQueryResponse(ProxyStatement(response.stmt.id, response.stmt.type),
                    response.columnMetaCount, columns)
        }
    }
}

class ProxyColumnResponse(val pos: Int, val name: String, val type: Int, val value: Any?=null)
    : ProxyMessage {
    private val response: ResponseOuterClass.ColumnResponse by lazy {
        val anyValue: Message? = when (JDBCType.valueOf(type)!!) {
            JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER -> Int32Value.of(value as Int)
            JDBCType.BIGINT -> Int64Value.of(value as Long)
            JDBCType.FLOAT -> FloatValue.of(value as Float)
            JDBCType.DECIMAL, JDBCType.NUMERIC, JDBCType.DOUBLE -> DoubleValue.of(value as Double)
            JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR -> StringValue.of(value as String)
            JDBCType.BOOLEAN -> BoolValue.of(value as Boolean)
            JDBCType.NULL -> null
            else -> TODO()
//            JDBCType.BIT -> TODO()
//            JDBCType.REAL -> TODO()
//            JDBCType.DATE -> TODO()
//            JDBCType.TIME -> TODO()
//            JDBCType.TIMESTAMP -> TODO()
//            JDBCType.BINARY -> TODO()
//            JDBCType.VARBINARY -> TODO()
//            JDBCType.LONGVARBINARY -> TODO()
//            JDBCType.OTHER -> TODO()
//            JDBCType.JAVA_OBJECT -> TODO()
//            JDBCType.DISTINCT -> TODO()
//            JDBCType.STRUCT -> TODO()
//            JDBCType.ARRAY -> TODO()
//            JDBCType.BLOB -> TODO()
//            JDBCType.CLOB -> TODO()
//            JDBCType.REF -> TODO()
//            JDBCType.DATALINK -> TODO()
//            JDBCType.ROWID -> TODO()
//            JDBCType.NCHAR -> TODO()
//            JDBCType.NVARCHAR -> TODO()
//            JDBCType.LONGNVARCHAR -> TODO()
//            JDBCType.NCLOB -> TODO()
//            JDBCType.SQLXML -> TODO()
//            JDBCType.REF_CURSOR -> TODO()
//            JDBCType.TIME_WITH_TIMEZONE -> TODO()
//            JDBCType.TIMESTAMP_WITH_TIMEZONE -> TODO()
        }
        ResponseOuterClass.ColumnResponse.newBuilder().setPos(pos).setName(name).setType(type).apply {
            anyValue?.let { setValue(ProtobufAny.pack(anyValue)) }
        }.build()
    }

    inline fun <reified T> getValueByType(): T =
            if (value != null && value is T) value
            else throw java.lang.IllegalArgumentException("unsupported type")

    override fun toProtoBuf() = response

    companion object {
        fun fromProtoBuf(response: ResponseOuterClass.ColumnResponse): ProxyColumnResponse {
            val value: Any? = when (JDBCType.valueOf(response.type)!!) {
                JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER -> {
                    response.value.unpack(Int32Value::class.java).value
                }
                JDBCType.BIGINT -> response.value.unpack(Int64Value::class.java).value
                JDBCType.FLOAT -> response.value.unpack(FloatValue::class.java).value
                JDBCType.DECIMAL, JDBCType.NUMERIC, JDBCType.DOUBLE -> {
                    response.value.unpack(DoubleValue::class.java).value
                }
                JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR -> {
                    response.value.unpack(StringValue::class.java).value
                }
                JDBCType.BOOLEAN -> response.value.unpack(BoolValue::class.java).value
                JDBCType.NULL -> null
                else -> TODO()
//                JDBCType.BIT -> TODO()
//                JDBCType.REAL -> TODO()
//                JDBCType.DATE -> TODO()
//                JDBCType.TIME -> TODO()
//                JDBCType.TIMESTAMP -> TODO()
//                JDBCType.BINARY -> TODO()
//                JDBCType.VARBINARY -> TODO()
//                JDBCType.LONGVARBINARY -> TODO()
//                JDBCType.OTHER -> TODO()
//                JDBCType.JAVA_OBJECT -> TODO()
//                JDBCType.DISTINCT -> TODO()
//                JDBCType.STRUCT -> TODO()
//                JDBCType.ARRAY -> TODO()
//                JDBCType.BLOB -> TODO()
//                JDBCType.CLOB -> TODO()
//                JDBCType.REF -> TODO()
//                JDBCType.DATALINK -> TODO()
//                JDBCType.ROWID -> TODO()
//                JDBCType.NCHAR -> TODO()
//                JDBCType.NVARCHAR -> TODO()
//                JDBCType.LONGNVARCHAR -> TODO()
//                JDBCType.NCLOB -> TODO()
//                JDBCType.SQLXML -> TODO()
//                JDBCType.REF_CURSOR -> TODO()
//                JDBCType.TIME_WITH_TIMEZONE -> TODO()
//                JDBCType.TIMESTAMP_WITH_TIMEZONE -> TODO()
            }
            return ProxyColumnResponse(response.pos, response.name, response.type, value)
        }
    }
}

interface ResponseHolder {
    fun write(writeFun: (ProxyResponse) -> Unit)
}

class FlowResponseHolder(private val flow: Flow<ProxyResponse>) : ResponseHolder {
    @InternalCoroutinesApi
    override fun write(writeFun: (ProxyResponse) -> Unit) {
        val job = GlobalScope.launch(Dispatchers.Unconfined) {
            flow.collect { msg -> writeFun(msg) }
        }
        runBlocking { job.join() }
    }
}
