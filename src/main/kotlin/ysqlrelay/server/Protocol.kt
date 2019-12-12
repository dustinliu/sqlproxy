package ysqlrelay.server

import ysqlrelay.proto.Common.Result
import ysqlrelay.proto.Response.SqlResponse as RelayResponse
import ysqlrelay.proto.Response.SqlResponse.Builder as RelayResponseBuilder

fun getResponseBuilder(code: Result.StatusCode=Result.StatusCode.SUCCESS, message: String?): RelayResponseBuilder? {
    val resultBuilder = Result.newBuilder().setCode(Result.StatusCode.SUCCESS)
    if (message != null) {
        resultBuilder.message = message
    }

    return RelayResponse.newBuilder().setResult(resultBuilder.build())
}
