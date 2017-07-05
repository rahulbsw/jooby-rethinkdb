package org.jooby.rethinkdb;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.jooby.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Created by rjain on 7/4/17.
 */
public class RethinkSessionStore implements Session.Store{
    private static final char DOT = '.';

    private static final char UDOT = '\uFF0E';

    private static final char DOLLAR = '$';

    private static final char UDOLLAR = '\uFF04';

    private static final String SESSION_IDX = "_sessionIdx_";

    /** The logging system. */
    private final Logger log = LoggerFactory.getLogger(getClass());


    private final AtomicBoolean ttlSync = new AtomicBoolean(false);

    private final RethinkDB r = RethinkDB.r;
    private final long timeout;
    private Connection connection=null;
    private Table table=null;

    public RethinkSessionStore(final String dbname, final String tablename,final String hostname,final int port,
                             final long timeoutInSeconds) {
        String db = requireNonNull(dbname, "Rethink db is required.");
        String table = requireNonNull(tablename, "tablename is required.");
        String host = requireNonNull(hostname, "hostname is required.");
        this.timeout = timeoutInSeconds;
        this.connection=r.connection().hostname(host).port(port).connect();

        if(!(Boolean)(r.dbList().contains(db).run(connection)))
        {
            r.dbCreate(db).run(connection);
        }

        if(!(Boolean)(r.db(db).tableList().contains(tablename).run(connection)))
        {
            r.db(db).tableCreate(tablename).run(connection);
            r.table(table).indexCreate(SESSION_IDX,"accessedAt").run(connection);
        }
        this.table=r.db(db).table(table);
    }

    @Inject
    public RethinkSessionStore(final @Named("rethinkdb.session.db") String db,
                               final @Named("rethinkdb.session.table") String tablename,
                               final @Named("rethinkdb.session.hostname") String hostname,
                               final @Named("rethinkdb.session.port") int port,
                               final @Named("session.timeout") String timeout) {
        this(db, tablename,hostname, port,seconds(timeout));
    }

    @Inject
    public RethinkSessionStore(final @Named("rethinkdb.session.db") String db,
                               final @Named("rethinkdb.session.table") String tablename,
                               final @Named("rethinkdb.session.hostname") String hostname,
                               final @Named("session.timeout") String timeout) {
        this(db, tablename,hostname,28015, seconds(timeout));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Session get(final Session.Builder builder) {
        Map session=table.get(builder.sessionId()).run(connection);
        if(!(isNull(session)|| session.isEmpty())){
        Long accessedAt = (Long) session.remove("accessedAt");
        Long createdAt = (Long) session.remove("createdAt");
        Long savedAt = (Long) session.remove("savedAt");
        String id =(String) session.remove("id");
        builder
         .accessedAt(accessedAt)
         .createdAt(createdAt)
         .savedAt(savedAt);
            System.out.println(session.keySet());
            session.forEach((k,v)->builder.set(decode(k.toString()), v.toString()));
         return builder.build();
        }
         return null;
     }

    @Override
    public void save(final Session session) {
        //syncTtl();
        MapObject doc=r.hashMap()
                .with("id", session.id())
                .with("accessedAt", session.accessedAt())
                .with("createdAt", session.createdAt())
                .with("savedAt", session.savedAt());
        // dump attributes
        Map<String, String> attributes = session.attributes();
        attributes.forEach((k, v) -> doc.put(encode(k), v));

        table.get(session.id()).replace(doc).run(connection);
    }

    @Override
    public void create(final Session session) {
        save(session);
    }

    private void syncTtl() {
        if (!ttlSync.get()) {
            ttlSync.set(true);

            if (timeout <= 0) {
                return;
            }

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

            ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
                Long ttlTime=System.currentTimeMillis()-(timeout*1000);
                table.filter(r.row("accessedAt").le(ttlTime)).delete().run(connection);
            }, 0, 10, TimeUnit.MINUTES);


        }
    }

    @Override
    public void delete(final String id) {
        table.get(id).delete().run(connection);
    }

    private static long seconds(final String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            Config config = ConfigFactory.empty()
                    .withValue("timeout", ConfigValueFactory.fromAnyRef(value));
            return config.getDuration("timeout", TimeUnit.SECONDS);
        }
    }

    private String encode(final String key) {
        String value = key;
        if (value.charAt(0) == DOLLAR) {
            value = UDOLLAR + value.substring(1);
        }
        return value.replace(DOT, UDOT);
    }

    private String decode(final String key) {
        String value = key;
        if (value.charAt(0) == UDOLLAR) {
            value = DOLLAR + value.substring(1);
        }
        return value.replace(UDOT, DOT);
    }
}
