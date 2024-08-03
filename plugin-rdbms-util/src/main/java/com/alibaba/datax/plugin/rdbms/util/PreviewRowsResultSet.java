package com.alibaba.datax.plugin.rdbms.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSet;


/**
 * 通过控制resultSet迭代next() 返回来控制迭代
 * https://github.com/apache/commons-dbutils/blob/master/src/main/java/org/apache/commons/dbutils/wrappers/StringTrimmedResultSet.java
 * Wraps a {@code ResultSet} to trim strings returned by the
 * {@code getString()} and {@code getObject()} methods.
 *
 * <p>
 * Usage Example:
 * This example shows how to decorate ResultSets so processing continues as
 * normal but all Strings are trimmed before being returned from the
 * {@code ResultSet}.
 * </p>
 *
 * <pre>
 * ResultSet resultSet = // somehow get a ResultSet;
 *
 * // Substitute wrapped ResultSet with additional behavior for real ResultSet
 * resultSet = StringTrimmedResultSet.wrap(resultSet);
 *
 * // Pass wrapped ResultSet to processor
 * List list = new BasicRowProcessor().toBeanList(resultSet);
 * </pre>
 */
public class PreviewRowsResultSet implements InvocationHandler {

    /**
     * Wraps the {@code ResultSet} in an instance of this class.  This is
     * equivalent to:
     * <pre>
     * ProxyFactory.instance().createResultSet(new StringTrimmedResultSet(resultSet));
     * </pre>
     *
     * @param resultSet The {@code ResultSet} to wrap.
     * @param pageSize  查询记录条数
     * @return wrapped ResultSet
     */
    public static ResultSet wrap(final ResultSet resultSet, int pageSize) {
        return ProxyFactory.instance().createResultSet(new PreviewRowsResultSet(resultSet, pageSize));
    }

    /**
     * The wrapped result.
     */
    private final ResultSet resultSet;
    private final int pageSize;
    private int nextExecuteCount = 0;

    /**
     * Constructs a new instance of {@code StringTrimmedResultSet}
     * to wrap the specified {@code ResultSet}.
     *
     * @param resultSet ResultSet to wrap
     */
    public PreviewRowsResultSet(final ResultSet resultSet, int pageSize) {
        this.resultSet = resultSet;
        this.pageSize = pageSize;
    }

    /**
     * Intercept calls to the {@code getString()} and
     * {@code getObject()} methods and trim any Strings before they're
     * returned.
     *
     * @param proxy  Not used; all method calls go to the internal result set
     * @param method The method to invoke on the result set
     * @param args   The arguments to pass to the result set
     * @return string trimmed result
     * @throws Throwable error
     * @see java.lang.reflect.InvocationHandler#invoke(Object, java.lang.reflect.Method, Object[])
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
            throws Throwable {

        Object result = method.invoke(this.resultSet, args);

        // 判断是否是next 方法调用
        // boolean next() throws SQLException;
        if (result instanceof Boolean
                && (method.getName().equals("next"))) {
            if (nextExecuteCount++ < pageSize) {
                return result;
            } else {
                // 终止记录集收集
                return false;
            }
        }

        return result;
    }

}
