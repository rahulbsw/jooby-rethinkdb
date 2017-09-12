package org.jooby.rethinkdb;

import com.google.inject.Inject;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;

import java.util.Optional;

import static com.rethinkdb.RethinkDB.r;
import static java.util.Objects.requireNonNull;

public class RethinkdbConnection {
    private Connection conn=null;
    private String defaultDbName=null;


    @Inject
    public RethinkdbConnection(Connection conn){
        requireNonNull(conn,"Connection is null");
        this.conn= conn;
        if(conn.isOpen())
        {
            reconnect();
        }
        defaultDbName=conn.db().isPresent()?conn.db().get():"test";
    }

    public <T> T run(ReqlAst reqlAst) {
        return conn.run(reqlAst, new OptArgs(), Optional.empty());
    }

    public <T> T run(ReqlAst reqlAst, OptArgs runOpts) {
        return conn.run(reqlAst, runOpts, Optional.empty());
    }

    public <T, P> T run(ReqlAst reqlAst, Class<P> pojoClass) {
        reconnect();
        return conn.run(reqlAst, new OptArgs(), Optional.of(pojoClass));
    }

    public <T, P> T run(ReqlAst reqlAst, OptArgs runOpts, Class<P> pojoClass) {
        reconnect();
        return conn.run(reqlAst, runOpts, Optional.of(pojoClass));
    }

    public void runNoReply(ReqlAst reqlAst) {
        reconnect();
        conn.runNoReply(reqlAst, new OptArgs());
    }

    public void runNoReply(ReqlAst reqlAst, OptArgs globalOpts) {
        reconnect();
        conn.runNoReply(reqlAst, globalOpts);
    }

    public void reconnect(){
        if(!conn.isOpen())
              conn.reconnect();
    }

    public boolean isDbExists(String dbname){
        return (Boolean)(r.dbList().contains(dbname).run(conn));
    }

    public Db createDB(String dbname){
        if(!isDbExists(dbname))
        {
             r.dbCreate(dbname).run(conn);
        }
        return r.db(dbname);
    }

    public void dropDB(String dbname){
        if(isDbExists(dbname))
        {
            r.dbDrop(dbname).run(conn);
        }
    }

    public boolean isTableExists(Db db,String tablename){
        return (Boolean)(db.tableList().contains(tablename).run(conn));
    }

    public Table createTable(Db db, String tablename){
        if(!isTableExists(db, tablename))
        {
            db.tableCreate(tablename).run(conn);
        }
        return db.table(tablename);
    }

    public boolean isTableExists(String dbname,String tablename){
        return isTableExists(createDB(dbname),tablename);
    }

    public Table createTable(String dbname, String tablename){
      return  createTable(createDB(dbname),tablename);
    }


    public void close() {
        conn.close();
    }

    public String getDefaultDbName() {
        return defaultDbName;
    }


    //   public class RethinkdbQuery {

//
//    protected RethinkdbQuery(String dbname){
//          Db db =run(RethinkDB.r.db(dbname));
//        }
////        public query(String dbname){
////            //RethinkDB.r
////        }
//    }



}
