package sqlproxy.server

import java.util.*
import javax.management.openmbean.CompositeData
import javax.management.openmbean.CompositeDataSupport
import javax.management.openmbean.CompositeType
import javax.management.openmbean.SimpleType

interface SessionInfoMBean {
    fun getSessionInfo(): Array<CompositeData>
}

class SessionInfo: SessionInfoMBean {
    override fun getSessionInfo(): Array<CompositeData> {
        val sessions = SessionFactory.getSessions()
        val dataList = arrayListOf<CompositeData>()
        sessions.forEach {
            val itemValues = arrayOf(it.id, Date(it.created))
            dataList.add(CompositeDataSupport(getSessionDataType(), arrayOf("id", "created"), itemValues))
        }
        return dataList.toTypedArray()
    }

    fun getSessionDataType() =
        CompositeType("session data type", "session data type",
            arrayOf("id", "created"), arrayOf("id", "created"),
            arrayOf(SimpleType.STRING, SimpleType.DATE))
}
