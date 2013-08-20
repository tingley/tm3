package com.globalsight.ling.tm3.core.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.jdbc.Work;

/**
 * Simple routines to simplify common SQL tasks.
 */
public class SQLUtil {

    private static Logger SQL_LOGGER = Logger.getLogger("com.globalsight.ling.tm3.core.SQL");
    
    private static final String myClassName = SQLUtil.class.getName();
    
    public static Logger getLogger() {
        return SQL_LOGGER;
    }
    
    /**
     * Return the ID of the last auto_inc field incremented in this connection.
     */
    public static long getLastInsertId(Session session) throws SQLException {
        return session.doReturningWork(new ReturningWork<Long>() {
            @Override
            public Long execute(Connection conn) throws SQLException {
                PreparedStatement ps = conn.prepareStatement("SELECT LAST_INSERT_ID()");
                logStatement(findLabel(), "SELECT LAST_INSERT_ID()");
                ResultSet rs = SQLUtil.execQuery(ps);
                rs.next();
                long l = rs.getLong(1);
                ps.close();
                log("LAST_INSERT_ID is " + l);
                return l;
            }
        });
    }
    
    /**
     * Exec a statement.  If logging is enabled, examine the stack to find the 
     * calling method.
     * @param conn
     * @param sql
     * @throws SQLException
     */
    public static void exec(Connection conn, String sql) throws SQLException {
        String label = null;
        exec(conn, sql, findLabel());
    }

    /**
     * Exec a statement, using the specified label for logging purposes.
     * @param conn
     * @param sql
     * @param label
     * @throws SQLException
     */
    public static void exec(Connection conn, String sql, String label) 
                                        throws SQLException {
        logStatement(label, sql);
        Statement s = conn.createStatement();
        Timer t = new Timer();
        s.executeUpdate(sql);
        s.close();
        logTimer(t);
    }
       
    public static ResultSet execQuery(Statement statement, String sql) 
                                        throws SQLException {
        String label = null;
        return execQuery(statement, sql, findLabel());
    }
    
    public static ResultSet execQuery(Statement statement, String sql,
                                    String label) throws SQLException {
        logStatement(label, sql);
        Timer t = new Timer();
        ResultSet rs = statement.executeQuery(sql);
        logTimer(t);
        return rs;
    }
    
    public static ResultSet execQuery(PreparedStatement ps) 
                    throws SQLException {
        return execQuery(ps, findLabel());
    }
    
    public static ResultSet execQuery(PreparedStatement ps, String label) 
                    throws SQLException {
        logStatement(label, ps);
        Timer t = new Timer();
        ResultSet rs = ps.executeQuery();
        logTimer(t);
        return rs;
    }

    /**
     * Executes a query that whose ResultSet returns a single column
     * containing a long.  Return the row results as a List.
     */
    public static List<Long> execIdsQuery(Session session, StatementBuilder sb) 
                        throws SQLException {
        return execIdsQuery(session, sb, findLabel());
    }
    
    public static List<Long> execIdsQuery(Session session, final StatementBuilder sb, 
                        String label) throws SQLException {
        logStatement(label, sb);
        return session.doReturningWork(new ReturningWork<List<Long>>() {
            @Override
            public List<Long> execute(Connection conn) throws SQLException {
                PreparedStatement ps = sb.toPreparedStatement(conn);
                Timer t = new Timer();
                ResultSet rs = ps.executeQuery();
                List<Long> ids = new ArrayList<Long>();
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
                ps.close();
                logTimer(t);
                return ids;
            }
        });
    }
    
    /**
     * Executes a query that whose ResultSet returns a single column.
     * Return the row results as a List.
     */
    public static List<Object> execObjectsQuery(Session session, StatementBuilder sb) 
                        throws SQLException {
        return execObjectsQuery(session, sb, findLabel());
    }
    
    public static List<Object> execObjectsQuery(Session session, final StatementBuilder sb, 
                        String label) throws SQLException {
        logStatement(label, sb);
        return session.doReturningWork(new ReturningWork<List<Object>>() {

            @Override
            public List<Object> execute(Connection conn) throws SQLException {
                PreparedStatement ps = sb.toPreparedStatement(conn);
                Timer t = new Timer();
                ResultSet rs = ps.executeQuery();
                List<Object> ids = new ArrayList<Object>();
                while (rs.next()) {
                    ids.add(rs.getObject(1));
                }
                ps.close();
                logTimer(t);
                return ids;
            }
        });
    }
    
    /**
     * Executes a query that whose ResultSet returns a single row consisting 
     * of a single column containing a long.  Return this value as a long.
     * @return single row value, or 0 if no results were returned
     */
    public static long execCountQuery(Session session, StatementBuilder sb) 
                        throws SQLException {
        return execCountQuery(session, sb, findLabel());
    }
    
    public static long execCountQuery(Session session, final StatementBuilder sb, 
                        String label) throws SQLException {
        logStatement(label, sb);
        return session.doReturningWork(new ReturningWork<Long>() {
            @Override
            public Long execute(Connection conn) throws SQLException {
                PreparedStatement ps = sb.toPreparedStatement(conn);
                Timer t = new Timer();
                ResultSet rs = ps.executeQuery();
                long count = 0;
                if (rs.next()) {
                    count = rs.getLong(1);
                }
                ps.close();
                logTimer(t);
                return count;
            }
        }).longValue();
    }
    
    /**
     * Executes a batch PreparedStatement with executeUpdate() and then closes
     * the statement.
     * @param conn
     * @param sb
     * @throws SQLException
     */
    public static void execBatch(Session session, BatchStatementBuilder sb) 
                throws SQLException { 
        execBatch(session, sb, findLabel());
    }
    
    public static void execBatch(Session session, final BatchStatementBuilder sb, String label) 
            throws SQLException {
        sb.setRequestKeys(false);
        logStatement(label, sb);
        session.doWork(new Work() {
            @Override
            public void execute(Connection conn) throws SQLException {
            PreparedStatement ps = sb.toPreparedStatement(conn);
            Timer t = new Timer();
            ps.executeBatch();
            logTimer(t);
            ps.close();
            }
        });
    }
    
    /**
     * Exec the statement contained in a StatementBuilder.  If logging is
     * enabled, examine the stack to find the calling method.
     * @param conn
     * @param sb
     * @throws SQLException
     */
    public static void exec(Session session, AbstractStatementBuilder sb) 
                        throws SQLException {
        exec(session, sb, findLabel());
    }
    
    /**
     * Exec the statement contained in a Statement builder, using the 
     * specified label for logging purposes.
     * @param conn
     * @param sb
     * @param label
     * @throws SQLException
     */
    public static void exec(Session session, final AbstractStatementBuilder sb, 
                        String label) throws SQLException {
        logStatement(label, sb);
        session.doWork(new Work() {
            @Override
            public void execute(Connection conn) throws SQLException {
                exec(conn, sb);
            }
        });
    }
    
    public static void exec(Connection conn, AbstractStatementBuilder sb)
                        throws SQLException {
        PreparedStatement ps = sb.toPreparedStatement(conn);
        Timer t = new Timer();
        ps.execute();
        logTimer(t);
        ps.close();
    }
    
    // Does not check for empty list
    public static StringBuilder longGroup(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(" (")
          .append(ids.get(0));
        for (int i = 1; i < ids.size(); i++) {
            sb.append(", ")
              .append(ids.get(i));
        }
        sb.append(") ");
        return sb;
    }
       
    private static void logStatement(String label, Statement s) {
        log(label + ">> " + s);
    }
    private static void logStatement(String label, AbstractStatementBuilder sb) {
        log(label + ">> " + sb);
    }
    private static void logStatement(String label, String s) {
        log(label + ">> " + s);
    }
    private static void logTimer(Timer timer) {
        log("Operation took " + timer.getDuration() + "ms");
    }
    private static void log(String s) {
        SQL_LOGGER.debug(s);
    }
    
    private static String findLabel() {
        if (! SQL_LOGGER.isDebugEnabled()) {
            return null;
        }
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        // Walk upwards to find the first non-SQLUtil caller (skip 0, which is Thread)
        for (int i = 1; i < stack.length; i++) { 
            if (!stack[i].getClassName().equals(myClassName)) {
                return stack[i].getMethodName();
            }
        }
        return "Unknown";
    }
    
    private static class Timer {
        private long start;
        Timer() {
            start = System.currentTimeMillis();
        }
        long getDuration() {
            return (System.currentTimeMillis() - start);
        }
    }
    
}
