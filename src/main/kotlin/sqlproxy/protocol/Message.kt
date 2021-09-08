package sqlproxy.protocol

sealed class SQLProxyMessage(val id: ULong)

class ConnectMessage(id: ULong, val user: String?, val password: String?): SQLProxyMessage(id)

class CloseMessage(id: ULong): SQLProxyMessage(id)
