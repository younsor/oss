package cn.zyy.oss.share;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class OssConfig
{
    private static final OssLog log = new OssLog();

    private static class ConfigLine
    {
        private String  lineContent = null;
        private boolean isValid     = false;
        private String  key         = null;
        private String  value       = null;

        public void updateLine()
        {
            lineContent = key.trim() + "=" + ((null == value) ? "" : value.trim());
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(lineContent).append("\n");
            sb.append("\t").append(isValid).append("\n");
            sb.append("\t").append("key=" + key).append("\n");
            sb.append("\t").append("value=" + value).append("\n");

            return sb.toString();
        }
    };

    private static final List<ConfigLine> fileConent = Lists.newArrayList();

    /* 需要调用者保证线程安全 */
    private static boolean readConfig(String filePath)
    {
        File file = new File(filePath);
        if (!file.exists() || file.isDirectory())
        {
            log.error("filePath(%s) not-exist or not-file", filePath);
            return false;
        }

        /* 读取文件内容, 不管是有效行、还是无效行，都记录下来 */
        BufferedReader buffReader = null;
        FileReader fileReader = null;
        fileConent.clear();
        try
        {
            fileReader = new FileReader(file);
            buffReader = new BufferedReader(fileReader);
            String tempString = null;
            while ((tempString = buffReader.readLine()) != null)
            {
                ConfigLine tmpLine = new ConfigLine();
                tmpLine.lineContent = tempString;
                fileConent.add(tmpLine);

                String strTrim = tempString.trim();
                if (strTrim.length() <= 0 || strTrim.startsWith("#"))
                {
                    continue;
                }

                String[] keyValue = strTrim.split("=");
                if (null == keyValue || keyValue.length != 2)
                {
                    continue;
                }

                tmpLine.isValid = true;
                tmpLine.key = keyValue[0].trim();
                tmpLine.value = keyValue[1].trim();
            }
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return false;
        }
        finally
        {
            if (null != fileReader)
            {
                try
                {
                    fileReader.close();
                }
                catch (Exception e1)
                {
                }
            }

            if (null != buffReader)
            {
                try
                {
                    buffReader.close();
                }
                catch (Exception e2)
                {
                }
            }
        }

        return true;
    }

    /* 需要调用者保证线程安全 */
    private static boolean writeConfig(String filePath)
    {
        File file = new File(filePath);
        if (file.exists() && file.isDirectory())
        {
            log.error("filePath(%s) exist, but it is a directory, cannot write content to it.");
            return false;
        }

        /* 文件不存在则创建空文件 */
        if (!file.exists())
        {
            try
            {
                if (!file.createNewFile())
                {
                    log.error("filePath(%s) not exist, but create new file fail.");
                    return false;
                }
            }
            catch (IOException e)
            {
                log.error("filePath(%s) not exist, but create new file exception.\n%s", OssFunc.getExceptionInfo(e));
                return false;
            }
        }

        FileWriter fw = null;
        BufferedWriter writer = null;
        try
        {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            for (ConfigLine tmpLine : fileConent)
            {
                writer.write(tmpLine.lineContent + "\n");
            }
            writer.flush();
            fw.flush();
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return false;
        }
        finally
        {
            try
            {
                if (null != writer)
                {
                    writer.close();
                }
            }
            catch (Exception e1)
            {
            }

            try
            {
                if (null != fw)
                {
                    fw.close();
                }
            }
            catch (Exception e2)
            {
            }
        }

        return true;
    }

    /* 配置文件格式如下: 
    # 写日志缓冲区大小，单位: KB；范围[1, 1024]；默认256KB
    BuffSize=256
    
    # 进程的oss、app、info日志文件最大大小, 单位: M；范围[1,256]；默认16M
    OssFileSize=16
    AppFileSize=16
    InfoFileSize=16
    */
    private static Map<String, String> getPropertiesToMap(String fileName)
    {
        Properties properties = new Properties();
        Map<String, String> propertyMap = new HashMap<String, String>();

        synchronized (fileConent)
        {
            File file = new File(fileName);
            if (!file.exists() || file.isDirectory())
            {
                return null;
            }

            InputStream inputFile = null;
            try
            {
                inputFile = new FileInputStream(file);
                properties.load(inputFile);
            }
            catch (IOException e)
            {
                log.error(OssFunc.getExceptionInfo(e));
            }
            finally
            {
                if (inputFile != null)
                {
                    try
                    {
                        inputFile.close();
                    }
                    catch (IOException e)
                    {
                        log.error(OssFunc.getExceptionInfo(e));
                    }
                }
            }

            Iterator<Entry<Object, Object>> it = properties.entrySet().iterator();
            while (it.hasNext())
            {
                Entry<Object, Object> entry = it.next();
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();
                propertyMap.put(key, value);
            }
        }

        return propertyMap;
    }

    public static boolean setPropertyValue(String filePath, String key, String value)
    {
        if (null == key)
        {
            log.error("setPropertyValue null == key");
            return false;
        }

        synchronized (fileConent)
        {
            fileConent.clear();
            if (!readConfig(filePath))
            {
                return false;
            }

            /* 找到key */
            boolean isFind = false;
            for (ConfigLine tmpLine : fileConent)
            {
                if (!tmpLine.isValid)
                {
                    continue;
                }

                if (!tmpLine.key.equalsIgnoreCase(key.trim()))
                {
                    continue;
                }

                /* 只更新 */
                tmpLine.value = value;
                tmpLine.updateLine();

                isFind = true;
                break;
            }

            if (!isFind)
            {
                ConfigLine newLine = new ConfigLine();
                newLine.isValid = true;
                newLine.key = key;
                newLine.value = value;
                newLine.updateLine();
                fileConent.add(newLine);

                log.info("setPropertyValue: add key=%s, value=%s in %s", key, value, filePath);
            }

            if (!writeConfig(filePath))
            {
                return false;
            }
        }

        return true;
    }

    public static int getIntValue(String fileName, String key, int defaultVal)
    {
        int re = defaultVal;
        String value = getPropertiesToMap(fileName).get(key);
        if (value != null)
        {
            re = Integer.parseInt(value);
        }
        return re;
    }

    public static long getLongValue(String fileName, String key, long defaultVal)
    {
        long re = defaultVal;
        String value = getPropertiesToMap(fileName).get(key);
        if (value != null)
        {
            re = Long.parseLong(value);
        }
        return re;
    }

    public static double getDoubleValue(String fileName, String key, double defaultVal)
    {
        double re = defaultVal;
        String value = getPropertiesToMap(fileName).get(key);
        if (value != null)
        {
            re = Double.parseDouble(value);
        }
        return re;
    }

    public static String getStringValue(String fileName, String key, String defaultVal)
    {
        String re = defaultVal;
        String value = getPropertiesToMap(fileName).get(key);
        if (value != null)
        {
            re = value.trim();
        }
        return re;
    }

    public static boolean getBooleanValue(String fileName, String key, boolean defaultVal)
    {
        boolean re = defaultVal;
        String value = getPropertiesToMap(fileName).get(key);
        if (value != null)
        {
            int intValue;
            try
            {
                intValue = Integer.parseInt(value);
                if (intValue > 0)
                {
                    re = true;
                }
                else
                {
                    re = false;
                }
            }
            catch (Exception e1)
            {
                try
                {
                    re = Boolean.parseBoolean(value);
                }
                catch (Exception e2)
                {
                    re = false;
                }
            }
        }

        return re;
    }

    /* 配置文件格式如下: 
    [Config]
    # 写日志缓冲区大小，单位: KB；范围[1, 1024]；默认256KB
    BuffSize=256
    
    # 进程的oss、app、info日志文件最大大小, 单位: M；范围[1,256]；默认16M
    OssFileSize=16
    AppFileSize=16
    InfoFileSize=16
    */
    private class ConfElem
    {
        public String note;
        public int    orderNo;
    }

    private class ConfNode
    {
        public String                note;
        public int                   orderNo;
        public Map<String, ConfElem> elemInfo = Maps.newHashMap();
    }

    private String                conf;
    private Map<String, ConfNode> nodeInfo = Maps.newHashMap();

    public OssConfig(String conf)
    {
        this.conf = conf;
    }

    public OssConfig(File confFile)
    {
        StringBuffer result = new StringBuffer();
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(confFile));
            String s = null;
            while ((s = br.readLine()) != null)
            {
                result.append(System.lineSeparator() + s);
            }
            br.close();
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }

        conf = result.toString();
    }

    private int resolve()
    {
        String lineSeparator = System.lineSeparator();
        String[] aStrLine = conf.split(lineSeparator);

        if (null == aStrLine || aStrLine.length <= 0)
        {
            return OssConstants.RET_ERROR;
        }

        String tmpStr = "";
        String nodeName = null;
        String nodeNote = null;
        String elemName = null;
        String elemNote = null;
        for (String strLine : aStrLine)
        {
            /* 去掉首尾的空白字符 */
            strLine = strLine.trim();
            if (strLine.length() <= 0)
            {
                continue;
            }

            /* 如果是注释行, 则存储注释信息 */
            if (strLine.startsWith("#"))
            {
                tmpStr += strLine.substring(1, strLine.length() - 1);
            }
        }

        return OssConstants.RET_OK;
    }
}
