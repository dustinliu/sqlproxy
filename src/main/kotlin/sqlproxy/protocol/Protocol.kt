package sqlproxy.protocol

import sqlproxy.grpc.Sqlproxy


fun getStatus(code: Sqlproxy.Status.StatusCode, message: String? = null): Sqlproxy.Status {
    val builder = Sqlproxy.Status.newBuilder().setCode(code)
    if (message != null) {
        builder.setMessage(message)
    }
    return builder.build()

}


fun getMeta(id: String, session: String?=null): Sqlproxy.Meta {
    val builder = Sqlproxy.Meta.newBuilder().setId(id)
    if (session != null) {
        builder.setSession(session)
    }
    return builder.build()
}

