package cn.zyy.oss.share;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class OssDB
{
    private static String           DB_URL_FORMAT = "jdbc:%s://%s:%d/?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&noAccessToProcedureBodies=true&Pooling=false&autoReconnect=true&connectTimeout=3000&maxReconnects=2";
    private static final OssLog     log           = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int        QUERY_TIMEOUT = 60 * 1000;
    private static final String     EMPTY_STRING  = "";
    private String                  driver        = "";
    private String                  ip            = "";
    private int                     port          = 0;
    private String                  userName      = "";
    private String                  passwd        = "";
    private Connection              conn          = null;
    private List<PreparedStatement> preStatList;

    public OssDB(String driver, String ip, int port, String userName, String passwd)
    {
        this.driver = driver;
        this.ip = ip;
        this.port = port;
        this.userName = userName;
        this.passwd = passwd;
        preStatList = Lists.newArrayList();
    }

    public class SelectWalker
    {
        private ResultSet rltSet      = null;
        ResultSetMetaData rsmd        = null;
        private int       columnCount = -1;

        public SelectWalker(ResultSet rltSet) throws SQLException
        {
            this.rltSet = rltSet;

            rsmd = rltSet.getMetaData();
            columnCount = rsmd.getColumnCount();
        }

        public boolean next() throws SQLException
        {
            return rltSet.next();
        }

        public void close() throws SQLException
        {
            try
            {
                closeResultSet(rltSet);
            }
            catch (Exception e)
            {
                log.error(OssFunc.getExceptionInfo(e));
            }
        }

        /**
         * 
         * @return never return null
         * @throws SQLException
         */
        public Map<String, Object> getRecord() throws SQLException
        {
            Map<String, Object> mapFields = Maps.newHashMap();
            for (int idx = 1; idx <= columnCount; idx++)
            {
                /* 如果值为null, 则转换为空字符串 */
                String columnName = rsmd.getColumnLabel(idx);
                Object columnValue = rltSet.getObject(idx);
                mapFields.put(columnName, (null == columnValue) ? EMPTY_STRING : columnValue);
            }

            return mapFields;
        }
    }

    public static class SelectResult
    {
        private Map<Map<String, String>, Map<String, Object>> mapRecord;

        public SelectResult()
        {
            mapRecord = Maps.newHashMap();
        }

        /**
         * 
         * @param order linkedHashMap OR HashMap
         */
        public SelectResult(boolean order)
        {
            if (order)
            {
                mapRecord = Maps.newLinkedHashMap();
            }
            else
            {
                mapRecord = Maps.newHashMap();
            }
        }

        public Map<Map<String, String>, Map<String, Object>> getMapRecord()
        {
            return mapRecord;
        }

        public static Map<String, String> newKey()
        {
            Map<String, String> dimFields = Maps.newTreeMap();
            return dimFields;
        }

        public Iterator getIterator()
        {
            return mapRecord.entrySet().iterator();
        }

        public int size()
        {
            return mapRecord.size();
        }

        public void close()
        {
            mapRecord.clear();
        }

        public void putRecord(Map<String, String> dimFields, Map<String, Object> tarFields)
        {
            mapRecord.put(dimFields, tarFields);
        }

        public SelectResult merge(SelectResult that)
        {
            mapRecord.putAll(that.mapRecord);
            return this;
        }

        public boolean isKeyExist(String... keyInfo)
        {
            if (null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2))
            {
                return false;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2)
            {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            return mapRecord.containsKey(dimFields);
        }

        public boolean isKeyExist(Map<String, String> dimFields)
        {
            if (null == dimFields)
            {
                return false;
            }

            return mapRecord.containsKey(dimFields);
        }

        public Map<String, Object> getRecordByKey(String... keyInfo)
        {
            if (null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2))
            {
                return null;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2)
            {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            return mapRecord.get(dimFields);
        }

        public Map<String, Object> getRecordByKey(Map<String, String> keyInfo)
        {
            if (null == keyInfo || keyInfo.size() <= 0)
            {
                return null;
            }
            return mapRecord.get(keyInfo);
        }

        public Object getFieldByKey(String fieldName, String... keyInfo)
        {
            Map<String, Object> mapValue = Maps.newHashMap();
            if (null == fieldName || null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2))
            {
                return null;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2)
            {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            mapValue = mapRecord.get(dimFields);
            if (null == mapValue)
            {
                return null;
            }

            return mapValue.get(fieldName);
        }

        public Integer getIntegerByKey(String fieldName, int defaultValue, String... keyInfo)
        {
            Map<String, Object> mapValue = Maps.newHashMap();
            if (null == fieldName || null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2))
            {
                return defaultValue;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2)
            {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            mapValue = mapRecord.get(dimFields);
            if (null == mapValue)
            {
                return defaultValue;
            }

            return (Integer) mapValue.get(fieldName);
        }

        public Object getFieldByKey(String fieldName, Map<String, String> dimFields)
        {
            Map<String, Object> mapValue = Maps.newHashMap();
            mapValue = mapRecord.get(dimFields);
            if (null == mapValue)
            {
                return null;
            }

            return mapValue.get(fieldName);
        }

        public String toString()
        {
            StringBuilder strInfo = new StringBuilder();
            for (Map<String, String> keys : mapRecord.keySet())
            {
                Map<String, Object> values = mapRecord.get(keys);
                strInfo.append("\t" + keys.toString() + "\t=\t" + values.toString() + "\n");
            }

            return strInfo.toString();
        }

        public int getInt(int defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            int iValue;
            if (objResult instanceof Boolean)
            {
                iValue = (Boolean.parseBoolean(objResult.toString())) ? 1 : 0;
            }
            else
            {
                iValue = OssFunc.DataConvert.toInt(objResult, defValue);
            }

            return iValue;
        }

        public long getLong(long defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            long lValue;
            if (objResult instanceof Boolean)
            {
                lValue = (Boolean.parseBoolean(objResult.toString())) ? 1 : 0;
            }
            else
            {
                lValue = OssFunc.DataConvert.toLong(objResult, defValue);
            }

            return lValue;
        }

        public float getFloat(float defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            float fValue;
            if (objResult instanceof Boolean)
            {
                fValue = (Boolean.parseBoolean(objResult.toString())) ? 1 : 0;
            }
            else
            {
                fValue = OssFunc.DataConvert.toFloat(objResult, defValue);
            }

            return fValue;
        }

        public double getDouble(double defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            double dValue;
            if (objResult instanceof Boolean)
            {
                dValue = (Boolean.parseBoolean(objResult.toString())) ? 1 : 0;
            }
            else
            {
                dValue = OssFunc.DataConvert.toDouble(objResult, defValue);
            }

            return dValue;
        }

        public BigDecimal getDecimal(long defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return BigDecimal.valueOf(defValue);
            }

            BigDecimal decimalValue = null;
            if (objResult instanceof BigDecimal)
            {
                return (BigDecimal) objResult;
            }
            else if (objResult instanceof Long)
            {
                return BigDecimal.valueOf((Long) objResult);
            }
            else if (objResult instanceof Integer)
            {
                return BigDecimal.valueOf((Long) objResult);
            }
            else if (objResult instanceof Double)
            {
                return BigDecimal.valueOf((Double) objResult);
            }
            else if (objResult instanceof Float)
            {
                return BigDecimal.valueOf((Float) objResult);
            }

            return BigDecimal.valueOf(defValue);
        }

        public String getString(String defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }
            return objResult.toString();
        }

        public boolean getBool(boolean defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            int iValue;
            if (objResult instanceof Boolean)
            {
                iValue = (Boolean.parseBoolean(objResult.toString())) ? 1 : 0;
            }
            else
            {
                iValue = OssFunc.DataConvert.toInt(objResult, 0);
            }

            return ((0 != iValue) ? true : false);
        }

        public long getTimeSecond(long defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            if (!(objResult instanceof Timestamp))
            {
                log.error("objResult instanceof Timestamp");
                return defValue;
            }

            Timestamp dbTimeStamp = (Timestamp) objResult;
            return dbTimeStamp.getTime() / 1000;
        }

        public long getTimeMillSecond(long defValue, String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return defValue;
            }

            if (!(objResult instanceof Timestamp))
            {
                log.error("objResult instanceof Timestamp");
                return defValue;
            }

            Timestamp dbTimeStamp = (Timestamp) objResult;
            return dbTimeStamp.getTime();
        }

        public List<Integer> getIntList(String fieldName, String... keyInfo)
        {
            List<Integer> lstIntValue = Lists.newArrayList();
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return lstIntValue;
            }

            /* 如果是空字符串, 则返回空; 否则外层认为有一个有效的空值, 这TM扯蛋 */
            String strValues = objResult.toString().trim();
            if (strValues.length() <= 0)
            {
                return lstIntValue;
            }

            String[] arrayValues = strValues.split(",");
            if (null == arrayValues || arrayValues.length <= 0)
            {
                return lstIntValue;
            }

            for (String tmpValue : arrayValues)
            {
                Integer iValue = OssFunc.DataConvert.toInt(tmpValue.trim(), -99999999);
                if (-99999999 == iValue)
                {
                    continue;
                }
                lstIntValue.add(iValue);
            }

            return lstIntValue;
        }

        public Set<Integer> getIntSet(String fieldName, String... keyInfo)
        {
            Set<Integer> setIntValue = Sets.newHashSet();
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return setIntValue;
            }

            /* 如果是空字符串, 则返回空; 否则外层认为有一个有效的空值, 这TM扯蛋 */
            String strValues = objResult.toString().trim();
            if (strValues.length() <= 0)
            {
                return setIntValue;
            }

            String[] arrayValues = strValues.split(",");
            if (null == arrayValues || arrayValues.length <= 0)
            {
                return setIntValue;
            }

            for (String tmpValue : arrayValues)
            {
                Integer iValue = OssFunc.DataConvert.toInt(tmpValue.trim(), -99999999);
                if (-99999999 == iValue)
                {
                    continue;
                }
                setIntValue.add(iValue);
            }

            return setIntValue;
        }

        public Set<String> getStringSet(String fieldName, String... keyInfo)
        {
            Set<String> SetStringValue = Sets.newHashSet();
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return SetStringValue;
            }

            /* 如果是空字符串, 则返回空; 否则外层认为有一个有效的空值, 这TM扯蛋 */
            String strValues = objResult.toString().trim();
            if (strValues.length() <= 0)
            {
                return SetStringValue;
            }

            String[] arrayValues = strValues.split(",");
            if (null == arrayValues || arrayValues.length <= 0)
            {
                return SetStringValue;
            }

            for (String tmpValue : arrayValues)
            {
                SetStringValue.add(tmpValue.trim());
            }

            return SetStringValue;
        }

        public List<String> getStringList(String fieldName, String... keyInfo)
        {
            List<String> lstStringValue = Lists.newArrayList();
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return lstStringValue;
            }

            /* 如果是空字符串, 则返回空; 否则外层认为有一个有效的空值, 这TM扯蛋 */
            String strValues = objResult.toString().trim();
            if (strValues.length() <= 0)
            {
                return lstStringValue;
            }

            String[] arrayValues = strValues.split(",");
            if (null == arrayValues || arrayValues.length <= 0)
            {
                return lstStringValue;
            }

            for (String tmpValue : arrayValues)
            {
                lstStringValue.add(tmpValue.trim());
            }

            return lstStringValue;
        }

        public Map<String, String> getMap(String fieldName, String... keyInfo)
        {
            Object objResult = getFieldByKey(fieldName, keyInfo);

            if (null == objResult)
            {
                return null;
            }

            Map<String, String> mapFeatures = Maps.newHashMap();
            String[] arrayFaeatures = objResult.toString().trim().split(",");
            for (String tmpFaeature : arrayFaeatures)
            {
                String[] tmpOneFaeature = tmpFaeature.split("=");
                if (null == tmpOneFaeature || tmpOneFaeature.length != 2)
                {
                    continue;
                }

                String faeatureKey = tmpOneFaeature[0].trim();
                String faeatureValue = tmpOneFaeature[1].trim();
                mapFeatures.put(faeatureKey.trim(), faeatureValue.trim());
            }

            return mapFeatures;
        }
    }

    public Connection getConnInst()
    {
        return conn;
    }

    public boolean getConnection()
    {
        String connUrl = String.format(DB_URL_FORMAT, this.driver, this.ip, this.port);
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(connUrl, this.userName, this.passwd);
            conn.setAutoCommit(false);
        }
        catch (Exception e)
        {
            log.error("Exception: \n" + OssFunc.getExceptionInfo(e));

            conn = null;
            return false;
        }
        return true;
    }

    public boolean isConnOk()
    {
        if (null == conn)
        {
            return false;
        }

        try
        {
            if (conn.isClosed())
            {
                return false;
            }

            if (!conn.isValid(0))
            {
                log.error("conn is invalid, and close it.");

                conn.close();

                return false;
            }
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return false;
        }

        return true;
    }

    public int[] executeBatch(List<String> sqlList) throws SQLException
    {
        if (null == sqlList || sqlList.size() <= 0)
        {
            return null;
        }
        Statement stmt = conn.createStatement();
        for (String tmpSql : sqlList)
        {
            stmt.addBatch(tmpSql);
        }
        int[] ret = stmt.executeBatch();
        stmt.close();
        return ret;
    }

    public ResultSet executeQuery(String sql) throws SQLException
    {
        return executeQuery(sql, false);
    }

    public ResultSet executeQuery(String sql, boolean isGroupConcatBig) throws SQLException
    {
        Statement execSql = conn.createStatement();

        if (isGroupConcatBig)
        {
            execSql.executeQuery("SET SESSION group_concat_max_len=512000");
        }

        execSql.setQueryTimeout(QUERY_TIMEOUT);
        return execSql.executeQuery(sql);
    }

    public boolean execute(String sql)
    {
        Statement stat = null;
        boolean ret = false;
        try
        {
            stat = conn.createStatement();
            ret = stat.execute(sql);
            stat.close();
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));

            try
            {
                if (null != stat)
                {
                    stat.close();
                }
            }
            catch (SQLException e1)
            {
                log.error(OssFunc.getExceptionInfo(e1));
            }

            ret = false;
        }

        return ret;
    }

    public int executeUpdate(String sql) throws SQLException
    {
        Statement stat = null;
        int iRet = 0;
        try
        {
            stat = conn.createStatement();
            iRet = stat.executeUpdate(sql);
            stat.close();
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));

            try
            {
                if (null != stat)
                {
                    stat.close();
                }
            }
            catch (SQLException e1)
            {
                log.error(OssFunc.getExceptionInfo(e1));
            }

            iRet = OssConstants.RET_ERROR;
        }

        return iRet;
    }

    public void addPreparedStatement(String sql) throws SQLException
    {
        preStatList.add(conn.prepareStatement(sql));
    }

    public int[] execPreparedStatement(int index) throws SQLException
    {
        if (index >= preStatList.size())
        {
            return null;
        }
        return preStatList.get(index).executeBatch();
    }

    public void execPreparedStatement() throws SQLException
    {
        for (PreparedStatement pre : preStatList)
        {
            pre.executeBatch();
        }
    }

    public void closePreparedStatement(int index) throws SQLException
    {
        if (index >= preStatList.size())
        {
            return;
        }
        preStatList.get(index).close();
    }

    public void closePreparedStatement() throws SQLException
    {
        for (PreparedStatement pre : preStatList)
        {
            pre.close();
        }
    }

    public void clearPreparedStatementBatch(int index) throws SQLException
    {
        if (index >= preStatList.size())
        {
            return;
        }
        preStatList.get(index).clearBatch();
    }

    public void clearPreparedStatementBatch() throws SQLException
    {
        for (PreparedStatement pre : preStatList)
        {
            pre.clearBatch();
        }
    }

    public void clearPreparedStatement()
    {
        preStatList.clear();
    }

    public void setPreparedStatement(int index, String... fields) throws SQLException
    {
        if (index >= preStatList.size())
        {
            return;
        }
        PreparedStatement pre = preStatList.get(index);
        for (int i = 1; i <= fields.length; ++i)
        {
            pre.setString(i, fields[i - 1]);
        }
        pre.addBatch();
    }

    public void setPreparedStatement(String... fields) throws SQLException
    {
        for (PreparedStatement pre : preStatList)
        {
            for (int i = 1; i <= fields.length; ++i)
            {
                pre.setString(i, fields[i - 1]);
            }
            pre.addBatch();
        }
    }

    public void setAutoCommit(boolean auto) throws SQLException
    {
        conn.setAutoCommit(auto);
    }

    public boolean commit()
    {
        try
        {
            conn.commit();
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return false;
        }

        return true;
    }

    public int rollback()
    {
        try
        {
            conn.rollback();
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return OssConstants.RET_ERROR;
        }

        return OssConstants.RET_OK;
    }

    public void closeConn()
    {
        try
        {
            conn.close();
        }
        catch (SQLException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }
    }

    public void closeResultSet(ResultSet rltSet)
    {
        try
        {
            if (!rltSet.isClosed())
            {
                Statement tmpSM = rltSet.getStatement();
                tmpSM.close();

                rltSet.close();
            }
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }
    }

    /**
     * 
     * @param sql
     * @return never return null
     * @throws Exception
     */
    public SelectWalker selectRowWalker(String sql) throws Exception
    {
        ResultSet rltSet = executeQuery(sql);
        return new SelectWalker(rltSet);
    }

    public SelectResult selectRowSet(String sql, Set<String> setDimFields) throws Exception
    {
        return selectRowSet(sql, setDimFields, false);
    }

    public SelectResult selectRowSet(String sql, Set<String> setDimFields, boolean isGroupConcatBig) throws Exception
    {
        ResultSet rltSet = executeQuery(sql, isGroupConcatBig);
        if (null == rltSet)
        {
            return null;
        }

        SelectResult selectResult = new SelectResult(false);
        SelectWalker walker = new SelectWalker(rltSet);
        while (walker.next())
        {
            Map<String, Object> mapFields = walker.getRecord();

            Map<String, String> dimFields = Maps.newTreeMap();
            Iterator<String> itSet = setDimFields.iterator();
            while (itSet.hasNext())
            {
                String dimFieldName = itSet.next();
                Object objLossDimField = mapFields.remove(dimFieldName);
                if (null == objLossDimField)
                {
                    log.error("dim-field[" + dimFieldName + "] loss in select-result");
                    continue;
                }
                dimFields.put(dimFieldName, objLossDimField.toString());
            }

            selectResult.putRecord(dimFields, mapFields);
        }
        walker.close();

        closeResultSet(rltSet);

        return selectResult;
    }

    public SelectResult selectOrderRowSet(String sql, Set<String> setDimFields) throws Exception
    {
        ResultSet rltSet = executeQuery(sql);
        if (null == rltSet)
        {
            return null;
        }

        SelectResult selectResult = new SelectResult(true);
        SelectWalker walker = new SelectWalker(rltSet);
        while (walker.next())
        {
            Map<String, Object> mapFields = walker.getRecord();

            Map<String, String> dimFields = Maps.newTreeMap();
            Iterator<String> itSet = setDimFields.iterator();
            while (itSet.hasNext())
            {
                String dimFieldName = itSet.next();
                dimFields.put(dimFieldName, mapFields.remove(dimFieldName).toString());
            }

            selectResult.putRecord(dimFields, mapFields);
        }
        walker.close();

        closeResultSet(rltSet);

        return selectResult;
    }

    /**
     * 
     * @param tableName
     * @param mapFields
     * @return either (1) the row count for SQL Data Manipulation Language (DML)
     *         statements or (2) 0 for SQL statements that return nothing or (3)
     *         -1 if execution throws Exception
     * @throws Exception
     */
    public int InsertRow(String tableName, Map<String, Object> mapFields) throws Exception
    {
        return InsertRow(tableName, mapFields, false);
    }

    public int InsertRow(String tableName, Map<String, Object> mapFields, boolean isIgnore) throws Exception
    {
        if (null == tableName || tableName.length() <= 0)
        {
            log.error("UpdateRow: tableName param error");
            return OssConstants.RET_ERROR;
        }

        if (null == mapFields || mapFields.size() <= 0)
        {
            log.error("UpdateRow: mapFields param error");
            return OssConstants.RET_ERROR;
        }

        boolean firstFlag = true;
        String strFields = "";
        String strValues = "";
        for (Map.Entry<String, Object> entry : mapFields.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                strFields += ", ";
                strValues += ", ";
            }

            strFields += ("`" + entry.getKey() + "`");
            strValues += "'" + entry.getValue() + "'";
        }

        String insertSql = null;
        if (isIgnore)
        {
            insertSql = "insert ignore into " + tableName + " (" + strFields + ") values (" + strValues + ")";
        }
        else
        {
            insertSql = "insert into " + tableName + " (" + strFields + ") values (" + strValues + ")";
        }

        int count = 0;
        try
        {
            count = executeUpdate(insertSql);
        }
        catch (Exception e)
        {
            log.error("insert exception: %s\n%s", insertSql, OssFunc.getExceptionInfo(e));
            return -1;
        }
        return count;
    }

    public int UpdateRow(String tableName, Map<String, String> dimMap, Map<String, Object> tarMap) throws Exception
    {
        if (null == tableName || tableName.length() <= 0)
        {
            log.error("UpdateRow: tableName param error");
            return OssConstants.RET_ERROR;
        }

        if (null == dimMap || dimMap.size() <= 0)
        {
            log.error("UpdateRow: dimMap param error");
            return OssConstants.RET_ERROR;
        }

        if (null == tarMap || tarMap.size() <= 0)
        {
            log.error("UpdateRow: tarMap param error");
            return OssConstants.RET_ERROR;
        }

        boolean firstFlag = true;
        String updateSql = "update " + tableName + " set ";
        for (Map.Entry<String, Object> entry : tarMap.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                updateSql += ", ";
            }

            updateSql += "`" + entry.getKey() + "`" + "='" + entry.getValue() + "'";
        }

        updateSql += " where ";

        firstFlag = true;
        for (Map.Entry<String, String> entry : dimMap.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                updateSql += " and ";
            }

            updateSql += entry.getKey() + "='" + entry.getValue() + "'";
        }

        int count = 0;
        try
        {
            count = executeUpdate(updateSql);
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return -1;
        }
        return count;
    }

    public int UpdateRowAddNum(String tableName, Map<String, String> dimMap, Map<String, Object> tarMap) throws Exception
    {
        if (null == tableName || tableName.length() <= 0)
        {
            log.error("UpdateRow: tableName param error");
            return OssConstants.RET_ERROR;
        }

        if (null == dimMap || dimMap.size() <= 0)
        {
            log.error("UpdateRow: dimMap param error");
            return OssConstants.RET_ERROR;
        }

        if (null == tarMap || tarMap.size() <= 0)
        {
            log.error("UpdateRow: tarMap param error");
            return OssConstants.RET_ERROR;
        }

        boolean firstFlag = true;
        String updateSql = "update " + tableName + " set ";
        for (Map.Entry<String, Object> entry : tarMap.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                updateSql += ", ";
            }

            updateSql += "`" + entry.getKey() + "`" + "=`" + entry.getKey() + "`+" + entry.getValue();
        }

        updateSql += " where ";

        firstFlag = true;
        for (Map.Entry<String, String> entry : dimMap.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                updateSql += " and ";
            }

            updateSql += entry.getKey() + "='" + entry.getValue() + "'";
        }

        int count = 0;
        try
        {
            count = executeUpdate(updateSql);
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return -1;
        }
        return count;
    }

    public int DeleteRow(String tableName, Map<String, Object> mapFields) throws Exception
    {
        if (null == tableName || tableName.length() <= 0)
        {
            log.error("UpdateRow: tableName param error");
            return OssConstants.RET_ERROR;
        }

        if (null == mapFields || mapFields.size() <= 0)
        {
            log.error("UpdateRow: mapFields param error");
            return OssConstants.RET_ERROR;
        }

        boolean firstFlag = true;
        String conditions = "";
        for (Map.Entry<String, Object> entry : mapFields.entrySet())
        {
            if (firstFlag)
            {
                firstFlag = false;
            }
            else
            {
                conditions += " and ";
            }

            conditions += entry.getKey() + "=" + "'" + entry.getValue() + "'";
        }

        String delSql = "delete from " + tableName + " where " + conditions;

        int count = 0;
        try
        {
            count = executeUpdate(delSql);
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return -1;
        }
        return count;
    }
}
