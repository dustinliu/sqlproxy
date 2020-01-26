package sqlproxy.server

import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import sqlproxy.client.ProxyClient
import sqlproxy.protocol.ProxyStmtResponse
import sqlproxy.protocol.ProxyUpdateResponse
import java.sql.Connection

object TestTable1 : IntIdTable("test_table1") {
    val name = varchar("name", 32)
}

internal class ServerTest {
    private val proxy = SQLProxy()

    @BeforeAll
    fun init() {
        Thread { proxy.start() }.start()
        proxy.awaitStart()

        initTestDataSource()

        Database.connect(DataSourceFactory.dataSource)
        TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    }

    @AfterAll
    fun destroy() {
        proxy.stop()
    }

    @BeforeEach
    fun setup() {
        transaction {
            SchemaUtils.drop(TestTable1)
            SchemaUtils.create(TestTable1)
        }
    }

    @AfterEach
    fun tearDown() {
        transaction { SchemaUtils.drop(TestTable1) }
        proxy.resetSessions()
    }

    @Test
    fun `normal connect and close test`() {
        val client = ProxyClient()
        val result1 = client.connect("localhost", 8888)
        val sessionId = result1.response.sessionId
        assertTrue(result1.response.isSuccessful())
        assertNotEquals(0, sessionId)
        assertEquals(result1.request.requestId, result1.response.requestId)
        assertEquals(1, proxy.getSessions().size)

        val result2 = client.close()
        assertTrue(result2.response.isSuccessful())
        assertEquals(sessionId, result2.request.sessionId)
        assertEquals(result2.request.requestId, result2.response.requestId)
        assertEquals(0, proxy.getSessions().size)
    }

    @Test
    fun `create stmt test`() {
        val client = ProxyClient()
        client.connect("localhost", 8888)
        val result = client.createStmt()
        assertTrue(result.response.isSuccessful())
        val stmtResponse = result.response.getSubResponseByType<ProxyStmtResponse>()
        client.close()
    }

    @Test
    fun `execute update test`() {
        val client = ProxyClient()
        client.connect("localhost", 8888)
        val stmtId = client.createStmt().response.getSubResponseByType<ProxyStmtResponse>().stmtId
        val result = client.execUpdate(stmtId, "insert into test_table1 (name) values ('nnn')")
        assertTrue(result.response.isSuccessful())
        assertEquals(1, result.response.getSubResponseByType<ProxyUpdateResponse>().count)
        val result1 by lazy {
            transaction { TestTable1.select { TestTable1.id eq 1 }.firstOrNull() }
        }
        assertEquals("nnn", result1?.get(TestTable1.name))
    }

//    @Test
//    fun `sql query`() {
//        transaction {
//            TestTable1.insert { it[name] = "1" }
//            TestTable1.insert { it[name] = "2" }
//            TestTable1.insert { it[name] = "3" }
//        }
//
//        val client = ProxyClient().apply { connect("localhost", 8888) }
//        val stmtId = client.createStmt().response.stmt.id
//        val response = client.execQuery(stmtId, "select id, name from test_table1").first().response as Response
//
//        assertEquals(Status.StatusCode.SUCCESS, response.status.code, response.status.message)
//        assertEquals(2, response.queryResponse.numOfColumn)
//        var column = response.queryResponse.columnMetaList[0]
//        assertEquals(1, column.pos)
//        assertEquals("id", column.name)
//        assertEquals(JDBCType.INTEGER, JDBCType.valueOf(column.type))
//
//        column = response.queryResponse.columnMetaList[1]
//        assertEquals(2, column.pos)
//        assertEquals("name", column.name)
//        assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(column.type))
//    }
}
