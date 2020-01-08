package sqlproxy.server

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.kodein.di.Kodein
import org.kodein.di.LateInitKodein
import org.kodein.di.generic.bind
import org.kodein.di.generic.provider
import javax.sql.DataSource

internal var kodein: Kodein = LateInitKodein()

val defaultKodein = Kodein {
    bind<Config>() with provider { ConfigFactory.load("sqlproxy") }
    bind<DataSource>() with provider { getConfigDataSource() }
}
