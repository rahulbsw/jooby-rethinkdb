package test.java.org.jooby.rethinkdb;

import java.util.concurrent.atomic.AtomicInteger;

import org.jooby.Jooby;
import org.jooby.Session;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class RethinkdbApp extends Jooby {

    {
        use(ConfigFactory.empty()
                .withValue("rethinkdb.session.db", ConfigValueFactory.fromAnyRef("rethinkdbapp"))
                .withValue("rethinkdb.session.table", ConfigValueFactory.fromAnyRef("session"))
                .withValue("rethinkdb.session.hostname", ConfigValueFactory.fromAnyRef("localhost"))
                .withValue("rethinkdb.session.port", ConfigValueFactory.fromAnyRef(28015))
                .withValue("session.timeout", ConfigValueFactory.fromAnyRef("2m")));

        use(new org.jooby.rethinkdb.Rethinkdb());

        AtomicInteger inc = new AtomicInteger(0);
        session(org.jooby.rethinkdb.RethinkSessionStore.class);

        get("/", req -> {
                Session session = req.ifSession().orElseGet(() -> {
                Session newSession = req.session();
                int next = newSession.get("inc").intValue(inc.getAndIncrement());
                newSession.set("inc", next);
                return newSession;
            });
            return session.get("inc");
        });



    }

    public static void main(final String[] args) {
        run(RethinkdbApp::new, args);
    }
}