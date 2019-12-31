package sqlproxy.server

import sqlproxy.proto.ResponseOuterClass
import ysqlrelay.proto.Common


//interface ProxyMessage  {
//    val id: String
//    val session: String?
//}
//
//class ProxyResponse(private val response: ResponseOuterClass.Response): ProxyMessage {
//    override val id: String = response.meta.id
//    override val session: String? = response.meta.session
//    val status = response.status
//}

class ResponseHolder(private val response: ResponseOuterClass.Response) {
    companion object {
        fun successBuilder(id: String, sessionId: String?=null): ResponseHolder {
            val protoBuilder = ResponseOuterClass.Response.newBuilder()
                .setStatus(ResponseOuterClass.Status.newBuilder().setCode(ResponseOuterClass.Status.StatusCode.SUCCESS))
            val meta = Common.Meta.newBuilder().setId(id).also {
                sessionId?.run { it.setSession(sessionId) }
            }.build()

            return ResponseHolder(protoBuilder.setMeta(meta).build())
        }

        fun failedResponse(id: String, sessionId: String?=null, msg: String?=null): ResponseHolder {
            val status = ResponseOuterClass.Status.newBuilder()
                .setCode(ResponseOuterClass.Status.StatusCode.FAILED).also {
                    msg?.run { it.setMessage(msg) }
                }.build()
            val meta = Common.Meta.newBuilder().setId(id).also {
                sessionId?.run { it.setSession(sessionId) }
            }.build()

            return ResponseHolder(ResponseOuterClass.Response.newBuilder().setStatus(status).setMeta(meta).build())
        }
    }

    fun writeAndFlush(writeFun: (ResponseOuterClass.Response)-> Unit) {
        writeFun(response)
    }
}
