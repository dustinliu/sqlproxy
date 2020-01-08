package sqlproxy.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.kodein.di.Kodein
import org.kodein.di.generic.bind
import org.kodein.di.generic.provider
import sqlproxy.client.ProxyClient
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.Status
import java.nio.file.Paths
import java.sql.Connection
import java.sql.JDBCType
import javax.sql.DataSource
import kotlin.test.assertNotNull

object TestTable1 : IntIdTable("test_table1") {
    val name = varchar("name", 32)
}

internal class ServerTest {
    private val proxy = SQLProxy()
    private val dataSource: DataSource by lazy {
        val config = HikariConfig()
        val dbFullName = Paths.get(System.getProperty("java.io.tmpdir"), "test.db").toString()
        with(config) {
            jdbcUrl = "jdbc:sqlite:$dbFullName"
        }
        HikariDataSource(config)
    }

    @BeforeAll
    fun init() {
        Thread { proxy.start() }.start()
        proxy.awaitStart()

        kodein = Kodein {
            extend(defaultKodein)
            bind<DataSource>(overrides = true) with provider { dataSource }
        }

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
        val sessionId = result1.response.meta.session
        assertEquals(Status.StatusCode.SUCCESS, result1.response.status.code)
        assertFalse(sessionId.isNullOrEmpty())
        assertEquals(result1.request.meta.id, result1.response.meta.id)
        assertEquals(1, proxy.getSessions().size)

        val result2 = client.close()
        assertEquals(Status.StatusCode.SUCCESS, result2.response.status.code)
        assertEquals(sessionId, result2.request.meta.session)
        assertEquals(result2.request.meta, result2.response.meta)
        assertEquals(0, proxy.getSessions().size)
    }

    @Test
    fun `create stmt test`() {
        val client = ProxyClient()
        client.connect("localhost", 8888)
        val result = client.createStmt()
        assertEquals(Status.StatusCode.SUCCESS, result.response.status.code)
        assertNotNull(result.response.stmt)
        assertFalse(result.response.stmt.id.isNullOrEmpty())
        client.close()
    }

    @Test
    fun `execute update test`() {
        val client = ProxyClient()
        client.connect("localhost", 8888)
        val stmtId = client.createStmt().response.stmt.id
        val result = client.execUpdate(stmtId, "insert into test_table1 (name) values ('nnn')")
        assertEquals(Status.StatusCode.SUCCESS, result.response.status.code, result.response.status.message)
        assertEquals(1, result.response.updateCount)
        val result1 by lazy {
            transaction { TestTable1.select { TestTable1.id eq 1 }.firstOrNull() }
        }
        assertEquals("nnn", result1?.get(TestTable1.name))
    }

    @Test
    fun `sql query`() {
        transaction {
            TestTable1.insert { it[name] = "1" }
            TestTable1.insert { it[name] = "2" }
            TestTable1.insert { it[name] = "3" }
        }

        val client = ProxyClient().apply { connect("localhost", 8888) }
        val stmtId = client.createStmt().response.stmt.id
        val response = client.execQuery(stmtId, "select id, name from test_table1").first().response as Response

        assertEquals(Status.StatusCode.SUCCESS, response.status.code, response.status.message)
        assertEquals(2, response.queryResponse.numOfColumn)
        var column = response.queryResponse.columnMetaList[0]
        assertEquals(1, column.pos)
        assertEquals("id", column.name)
        assertEquals(JDBCType.INTEGER, JDBCType.valueOf(column.type))

        column = response.queryResponse.columnMetaList[1]
        assertEquals(2, column.pos)
        assertEquals("name", column.name)
        assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(column.type))
    }
}
