package com.alibaba.datax.plugin.reader.ftpreader;

public interface Key {

    /**
     * 使用hive时候作为 hive表的表名使用
     */
    String ENTITY_NAME = "entityName";


    public static final String PROTOCOL = "protocol";
    public static final String HOST = "host";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String PORT = "port";
    public static final String TIMEOUT = "timeout";
    public static final String CONNECTPATTERN = "connectPattern";
    public static final String PATH = "path";
    public static final String MAXTRAVERSALLEVEL = "maxTraversalLevel";
}
