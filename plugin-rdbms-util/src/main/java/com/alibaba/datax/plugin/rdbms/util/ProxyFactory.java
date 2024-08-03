package com.alibaba.datax.plugin.rdbms.util;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-27 09:59
 **/
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Creates proxy implementations of JDBC interfaces.  This avoids
 * incompatibilities between the JDBC 2 and JDBC 3 interfaces.  This class is
 * thread safe.
 *
 * @see java.lang.reflect.Proxy
 * @see java.lang.reflect.InvocationHandler
 */
public class ProxyFactory {

    /**
     * The Singleton instance of this class.
     */
    private static final ProxyFactory INSTANCE = new ProxyFactory();

    /**
     * Returns the Singleton instance of this class.
     *
     * @return singleton instance
     */
    public static ProxyFactory instance() {
        return INSTANCE;
    }

    /**
     * Protected constructor for ProxyFactory subclasses to use.
     */
    protected ProxyFactory() {
    }

    /**
     * Creates a new proxy {@code CallableStatement} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied CallableStatement
     */
    public CallableStatement createCallableStatement(final InvocationHandler handler) {
        return newProxyInstance(CallableStatement.class, handler);
    }

    /**
     * Creates a new proxy {@code Connection} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied Connection
     */
    public Connection createConnection(final InvocationHandler handler) {
        return newProxyInstance(Connection.class, handler);
    }

    /**
     * Creates a new proxy {@code Driver} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied Driver
     */
    public Driver createDriver(final InvocationHandler handler) {
        return newProxyInstance(Driver.class, handler);
    }

    /**
     * Creates a new proxy {@code PreparedStatement} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied PreparedStatement
     */
    public PreparedStatement createPreparedStatement(final InvocationHandler handler) {
        return newProxyInstance(PreparedStatement.class, handler);
    }

    /**
     * Creates a new proxy {@code ResultSet} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied ResultSet
     */
    public ResultSet createResultSet(final InvocationHandler handler) {
        return newProxyInstance(ResultSet.class, handler);
    }

    /**
     * Creates a new proxy {@code ResultSetMetaData} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied ResultSetMetaData
     */
    public ResultSetMetaData createResultSetMetaData(final InvocationHandler handler) {
        return newProxyInstance(ResultSetMetaData.class, handler);
    }

    /**
     * Creates a new proxy {@code Statement} object.
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied Statement
     */
    public Statement createStatement(final InvocationHandler handler) {
        return newProxyInstance(Statement.class, handler);
    }

    /**
     * Convenience method to generate a single-interface proxy using the handler's classloader
     *
     * @param <T> The type of object to proxy
     * @param type The type of object to proxy
     * @param handler The handler that intercepts/overrides method calls.
     * @return proxied object
     */
    public <T> T newProxyInstance(final Class<T> type, final InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(handler.getClass().getClassLoader(), new Class<?>[] {type}, handler));
    }

}
