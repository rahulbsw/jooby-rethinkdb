package org.jooby.rethinkdb;

import com.google.inject.Binder;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooby.Env;
import org.jooby.Jooby;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Created by rjain on 7/4/17.
 */
public class Rethinkdb implements Jooby.Module  {
    private final String db;

    private BiConsumer<Connection.Builder, Config> options;

    public Config config() {
        return ConfigFactory.parseResources(Rethinkdb.class, "rethinkdb.conf");
    }

    public Rethinkdb(final String db) {
        this.db = requireNonNull(db, "A rethink db is required.");
    }

    public Rethinkdb(){
        this("db");
    }

    public void configure(Env env, Config config, Binder binder) throws Throwable {

    }

    protected void configure(final Env env, final Config config, final Binder binder,
                             final BiConsumer<Connection.Builder, Connection> callback) throws FileNotFoundException {
        Connection.Builder connectionBuilder = options(rethinkdb(config));

        if (this.options != null) {
            this.options.accept(connectionBuilder, config);
        }

        Connection connection=connectionBuilder.connect();
        String database=connection.db().get();


        Env.ServiceKey serviceKey = env.serviceKey();
        serviceKey.generate(Connection.Builder.class, database, k -> binder.bind(k).toInstance(connectionBuilder));
        serviceKey.generate(Connection.class, database, k -> binder.bind(k).toInstance(connection));

        env.onStop(()->connection.close());

        callback.accept(connectionBuilder, connection);
    }


    private Config rethinkdb(final Config config) {
        Config $rethinkdb = config.getConfig("rethinkdb");
        if (config.hasPath("rethinkdb." + db)) {
            $rethinkdb = config.getConfig("rethinkdb." + db).withFallback($rethinkdb);
        }
        return $rethinkdb;
    }

    /**
     * Set an options callback.
     *
     * <pre>
     * {
     *   use(new Rethinkdb()
     *     .options((options, config) {@literal ->} {
     *       options.db("rethinkdb");
     *     })
     *   );
     * }
     * </pre>
     *
     * @param options Configure callback.
     * @return This module
     */
    public Rethinkdb options(final BiConsumer<Connection.Builder, Config> options) {
        this.options = requireNonNull(options, "Options callback is required.");
        return this;
    }

    private Connection.Builder options(final Config config) throws FileNotFoundException {

        Connection.Builder builder=RethinkDB.r.connection()
          .db(isNull(config.getString("db"))?config.getString("db"):"test")
          .hostname(isNull(config.getString("hostname"))?config.getString("hostname"):"localhost")
          .port(isNull(config.getInt("port"))?config.getInt("port"):28015);

        if(!isNull(config.getDuration("timeout")))
            builder.timeout((int) config.getDuration("timeout", TimeUnit.SECONDS));
        if(!isNull(config.getString("user")))
            builder.user(config.getString("user"),config.getString("password"));
        if(!isNull(config.getString("authKey")))
            builder.authKey(config.getString("authKey"));
        if(!isNull(config.getString("certFile")))
            builder.certFile(new FileInputStream(config.getString("certFile")));

        return builder;
    }


}
