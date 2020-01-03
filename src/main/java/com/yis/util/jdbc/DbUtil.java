package com.yis.util.jdbc;

import com.yis.util.exception.BizException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author milu
 * @Description database
 * @createTime 2019年10月30日 15:42:00
 */
public class DbUtil {

    private final static Logger logger = LogManager.getLogger(DbUtil.class);

    private final static String driver = "com.mysql.cj.jdbc.Driver";
    private final static String url = "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";

    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    private static volatile DbUtil db;

    private DbUtil() {}

    /**
     * 初始化数据库
     * @param ip
     * @param port
     * @param dbName
     * @param user
     * @param password
     * @return
     */
    public static boolean initInstance(String ip, String port, String dbName, String user, String password) {
        if (db == null) {
            synchronized (DbUtil.class) {
                if (db == null) {
                    db = new DbUtil();
                    db.initConnection(ip, port, dbName, user, password);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 获取数据库实例
     * @return
     */
    public static DbUtil getInstance() {
        if (db == null) {
            synchronized (DbUtil.class) {
                if (db == null) {
                    logger.error("KafkaConsumer is not inited, please init");
                    throw new BizException("init KafkaConsumer first, use KafkaConsumer.initInstance method");
                }
            }
        }
        return db;
    }

    private void initConnection(String ip, String port, String dbName, String user, String password) {
        try {
            if (conn == null) {
                synchronized (DbUtil.class) {
                    if (conn == null) {
                        Class.forName(driver);
                        conn = DriverManager.getConnection(String.format(url, ip, port, dbName), user, password);
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            logger.error("DbUtil.getConnection ClassNotFoundException, error:{}", e);
        } catch (SQLException e) {
            logger.error("DbUtil.getConnection SQLException, error:{}", e);
        }
    }

    /**
     * 关闭数据库
     */
    public void closeDB() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("DbUtil.closeDB SQLException, error:{}", e);
        }
    }

    /**
     * sql执行
     * @param sql
     * @param params
     * @return
     */
    public List<Map<String, Object>> execute(String sql, Type type, Object ...params) {
        List<Map<String, Object>> res = new ArrayList<>();
        try {
            ps = conn.prepareStatement(sql);
            if (params != null && params.length != 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            if (type.isQuery()) {
                rs = ps.executeQuery();
                while (rs.next()) {
                    Map<String, Object> tmp = new HashMap<>();
                    for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                        tmp.put(rs.getMetaData().getColumnLabel(i + 1), rs.getObject(i + 1));
                    }
                    res.add(tmp);
                }
            } else {
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("DbUtil.executeQuery SQLException, error:{}", e);
        }
        return res;
    }

    /**
     * 批量sql执行
     * @param sql
     * @param type
     * @param params
     * @return
     */
    public List<Map<String, Object>> batchExecute(String sql, Type type, List<Object[]> params) {
        List<Map<String, Object>> res = new ArrayList<>();
        try {
            boolean batchUpdates = conn.getMetaData().supportsBatchUpdates();
            if (!batchUpdates) {
                logger.error("jdbc not support batch!");
                throw new RuntimeException("jdbc not support batch!");
            }
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(sql);
            for (Object[] param : params) {
                for (int i = 0; i < param.length; i++) {
                    ps.setObject(i + 1, param[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            logger.info("sql batch execute over, sql={}", sql);
        } catch (SQLException e) {
            logger.error("DbUtil.batchExecute SQLException, error:{}", e);
        }
        return res;
    }

    public enum Type {
        QUERY(true), CREATE(false), INSERT(false), DELETE(false);

        private boolean query;

        Type(boolean query) {
            this.query = query;
        }

        public boolean isQuery() {
            return query;
        }
    }

}
