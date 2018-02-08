package cn.zyy.oss.share;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Sets;

public class OssFunc
{
    private static final OssLog log = new OssLog();

    public static void sleep(int millSecond)
    {
        try
        {
            Thread.currentThread().sleep(millSecond);
        }
        catch (InterruptedException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }
    }

    public static void accurateSleep(long millSecond)
    {
        try
        {
            Thread.currentThread().sleep(millSecond, 0);
        }
        catch (InterruptedException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }
    }

    public static void nanoSleep(long nanoSecond)
    {
        long setMillSecond = nanoSecond / 1000000;
        long setNanoSecond = nanoSecond % 1000000;
        try
        {
            Thread.currentThread().sleep((int) setMillSecond, (int) setNanoSecond);
        }
        catch (InterruptedException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
        }
    }

    public static String Md5(String plainText)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes("UTF-8"));
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++)
            {
                i = b[offset];
                if (i < 0)
                {
                    i += 256;
                }

                if (i < 16)
                {
                    buf.append("0");
                }

                buf.append(Integer.toHexString(i));
            }
            return buf.toString();
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return "";
        }
    }

    public static String SHA1(String decrypt)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            digest.update(decrypt.getBytes());

            byte messageDigest[] = digest.digest();
            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < messageDigest.length; i++)
            {
                String shaHex = Integer.toHexString(messageDigest[i] & 0xFF);
                if (shaHex.length() < 2)
                {
                    hexString.append(0);
                }
                hexString.append(shaHex);
            }

            return hexString.toString();
        }
        catch (Exception e)
        {
            log.error("get sha1 exception: \n" + OssFunc.getExceptionInfo(e));

            return null;
        }
    }

    public static String getFileContent(String filePath)
    {
        StringBuilder jsonContent = new StringBuilder();

        InputStreamReader read = null;
        BufferedReader bufferedReader = null;
        try
        {
            String lineTxt = null;
            File file = new File(filePath);
            if (file.isFile() && file.exists())
            {
                read = new InputStreamReader(new FileInputStream(file));
                bufferedReader = new BufferedReader(read);

                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    jsonContent.append(lineTxt);
                }
                bufferedReader.close();
                read.close();
            }
            else
            {
                log.error("filePath not exist!");
            }
        }
        catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            log.error(" read %s error. \n%s", filePath, OssFunc.getExceptionInfo(e));

            try
            {
                if (null != bufferedReader)
                {
                    bufferedReader.close();
                }
            }
            catch (Exception e1)
            {
                log.error(" bufferedReader.close error. \n%s", filePath, OssFunc.getExceptionInfo(e1));
            }

            try
            {
                if (null != read)
                {
                    read.close();
                }
            }
            catch (Exception e2)
            {
                log.error(" read.close error. \n%s", filePath, OssFunc.getExceptionInfo(e2));
            }

            return null;
        }

        return jsonContent.toString();
    }

    public static int writeToFile(String filePath, String content, boolean isAppend)
    {
        File fWriterFile = new File(filePath);
        if (!fWriterFile.exists())
        {
            try
            {
                if (!fWriterFile.createNewFile())
                {
                    log.error("createNewFile " + filePath + " error.");
                    return OssConstants.RET_ERROR;
                }
            }
            catch (IOException e)
            {
                log.error("createNewFile " + filePath + " exception\n" + OssFunc.getExceptionInfo(e));
                return OssConstants.RET_ERROR;
            }
        }

        int writerNum = 0;
        FileWriter fw = null;
        BufferedWriter writerBuff = null;
        try
        {
            fw = new FileWriter(fWriterFile, isAppend);
            writerBuff = new BufferedWriter(fw);
            writerBuff.write(content);
            writerBuff.flush();
            fw.flush();
        }
        catch (Exception e)
        {
            log.error("write cache-file(%s) exception, has writer %s data. \n%s", fWriterFile, writerNum, OssFunc.getExceptionInfo(e));

            /* 异常处理. TODO */
            return OssConstants.RET_ERROR;
        }
        finally
        {
            if (null != writerBuff)
            {
                try
                {
                    writerBuff.close();
                }
                catch (IOException e)
                {
                    log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                }
            }

            if (null != fw)
            {
                try
                {
                    fw.close();
                }
                catch (IOException e)
                {
                    log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                }
            }
        }

        return OssConstants.RET_OK;
    }

    public static String getExceptionInfo(Exception e)
    {
        StringBuilder strBuff = new StringBuilder();
        if (null == e)
        {
            strBuff.append("exception-info=null\n");

            return strBuff.toString();
        }
        else
        {
            strBuff.append(e.toString() + "\n");
        }

        StackTraceElement[] trace = e.getStackTrace();
        if (null == trace)
        {
            strBuff.append("stack-trace=null\n");
        }
        else if (trace.length <= 0)
        {
            strBuff.append("stack-trace is empty\n");
        }
        else
        {
            for (StackTraceElement s : trace)
            {
                strBuff.append("\t" + s + "\n");
            }
        }

        return strBuff.toString();
    }

    public static String getExceptionInfo(Throwable e)
    {
        StringBuilder strBuff = new StringBuilder();
        if (null == e)
        {
            strBuff.append("exception-info=null\n");

            return strBuff.toString();
        }
        else
        {
            strBuff.append(e.toString() + "\n");
        }

        StackTraceElement[] trace = e.getStackTrace();
        if (null == trace)
        {
            strBuff.append("stack-trace=null\n");
        }
        else if (trace.length <= 0)
        {
            strBuff.append("stack-trace is empty\n");
        }
        else
        {
            for (StackTraceElement s : trace)
            {
                strBuff.append("\t" + s + "\n");
            }
        }

        return strBuff.toString();
    }

    public static String getMonitorUrls(List<String> lstUrl)
    {
        StringBuilder str = new StringBuilder();
        str.append("\"");
        if (null != lstUrl)
        {
            for (int idx = 0; idx < lstUrl.size(); idx++)
            {
                if (idx > 0)
                {
                    str.append(", ");
                }

                str.append(lstUrl.get(idx));
            }
        }
        str.append("\"");

        return str.toString();
    }

    public static long ipToLong(String strIp)
    {
        long ipValue = 0;

        try
        {
            long[] ip = new long[4];
            // 先找到IP地址字符串中.的位置
            int position1 = strIp.indexOf(".");
            int position2 = strIp.indexOf(".", position1 + 1);
            int position3 = strIp.indexOf(".", position2 + 1);
            // 将每个.之间的字符串转换成整型
            ip[0] = Long.parseLong(strIp.substring(0, position1));
            ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
            ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
            ip[3] = Long.parseLong(strIp.substring(position3 + 1));
            ipValue = (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
        }
        catch (Exception e)
        {
            log.error("ip[" + strIp + "] to long-value exception\n" + OssFunc.getExceptionInfo(e));
            ipValue = 0;
        }

        return ipValue;
    }

    // 将十进制整数形式转换成127.0.0.1形式的ip地址
    public static String longToIP(long longIp)
    {
        StringBuffer sb = new StringBuffer("");
        // 直接右移24位
        sb.append(String.valueOf((longIp >>> 24)));
        sb.append(".");
        // 将高8位置0，然后右移16位
        sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
        sb.append(".");
        // 将高16位置0，然后右移8位
        sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
        sb.append(".");
        // 将高24位置0
        sb.append(String.valueOf((longIp & 0x000000FF)));
        return sb.toString();
    }

    public static boolean isEmpty(Object object)
    {
        if (null == object)
        {
            return true;
        }

        String str = object.toString().trim();
        if (str.length() <= 0)
        {
            return true;
        }

        return false;
    }

    public static boolean isEmptyOrNull(Object object)
    {
        if (null == object)
        {
            return true;
        }

        String str = object.toString().trim();
        if (str.length() <= 0 || "null".equalsIgnoreCase(str))
        {
            return true;
        }

        return false;
    }

    public static class TimeConvert
    {
        public final static String DF_DAY          = "yyyy-MM-dd";
        public final static String DF_HOUR         = "yyyy-MM-dd HH";
        public final static String DF_MINUTE       = "yyyy-MM-dd HH:mm";
        public final static String DF_SECOND       = "yyyy-MM-dd HH:mm:ss";
        public final static String DF_MILL_SECOND  = "yyyy-MM-dd HH:mm:ss:SSS";
        public final static String SID_MILL_SECOND = "yyyyMMddHHmmssSSS";

        public static synchronized Date Format2Date(Object objDate, String format)
        {
            if (null == objDate)
            {
                return null;
            }

            String strDate = objDate.toString().trim();
            if (strDate.length() <= 0)
            {
                return null;
            }

            Date date = null;
            try
            {
                date = (new SimpleDateFormat(format)).parse(strDate);
            }
            catch (Exception e)
            {
                date = null;
                log.error(OssFunc.getExceptionInfo(e));
            }

            return date;
        }

        public static synchronized String Date2Format(Date date, String format)
        {
            return (new SimpleDateFormat(format)).format(date);
        }

        public static long millSecTrimMinute2Sec(long millSecond)
        {
            millSecond = millSecond / 1000;
            return (millSecond - millSecond % 60);
        }

        public static long millSecTrimHour2Sec(long millSecond)
        {
            millSecond = millSecond / 1000;
            return (millSecond - millSecond % 3600);
        }

        public static Date trimDay(Date date)
        {
            return OssFunc.TimeConvert.Format2Date(String.format("%4d-%02d-%-2d", date.getYear() + 1900, date.getMonth() + 1, date.getDate()), "yyyy-MM-dd");
        }

        public static Date trimHour(Date date)
        {
            return OssFunc.TimeConvert.Format2Date(String.format("%4d-%02d-%-2d %02d", date.getYear() + 1900, date.getMonth() + 1, date.getDate(), date.getHours()), "yyyy-MM-dd HH");
        }

        public static Date dbTimestamp2Day(Object objTimestamp, Date defDate)
        {
            if (null == objTimestamp)
            {
                log.debug("null == objTimestamp, need Timestamp's type");
                return defDate;
            }

            if (!(objTimestamp instanceof Timestamp))
            {
                log.error("objTimestamp not instanceof Timestamp");
                return defDate;
            }

            Timestamp dbEndTime = (Timestamp) objTimestamp;
            return OssFunc.TimeConvert.Format2Date(String.format("%4d-%02d-%-2d", dbEndTime.getYear() + 1900, dbEndTime.getMonth() + 1, dbEndTime.getDate()), "yyyy-MM-dd");
        }

        public static Date dbTimestamp2Hour(Object objTimestamp, Date defDate)
        {
            if (null == objTimestamp)
            {
                log.error("null == objTimestamp, need Timestamp's type");
                return defDate;
            }

            if (!(objTimestamp instanceof Timestamp))
            {
                log.error("objTimestamp not instanceof Timestamp");
                return defDate;
            }

            Timestamp dbEndTime = (Timestamp) objTimestamp;
            return OssFunc.TimeConvert.Format2Date(String.format("%4d-%02d-%-2d %02d", dbEndTime.getYear() + 1900, dbEndTime.getMonth() + 1, dbEndTime.getDate(), dbEndTime.getHours()), "yyyy-MM-dd HH");
        }

        public static int dateCpmDay(Date date1, Date date2)
        {
            if (date1.getYear() < date2.getYear())
            {
                return -1;
            }
            else if (date1.getYear() > date2.getYear())
            {
                return 1;
            }

            if (date1.getMonth() < date2.getMonth())
            {
                return -1;
            }
            else if (date1.getMonth() > date2.getMonth())
            {
                return 1;
            }

            if (date1.getDate() < date2.getDate())
            {
                return -1;
            }
            else if (date1.getDate() > date2.getDate())
            {
                return 1;
            }

            return 0;
        }

        public static int dateCpmHour(Date date1, Date date2)
        {
            int cpmValue = dateCpmDay(date1, date2);
            if (0 != cpmValue)
            {
                return cpmValue;
            }

            if (date1.getHours() < date2.getHours())
            {
                return -1;
            }
            else if (date1.getHours() > date2.getHours())
            {
                return 1;
            }

            return 0;
        }
    }

    public static String getPath(String baseDir, String fileDir)
    {
        baseDir = baseDir.trim();
        if (baseDir.charAt(baseDir.length() - 1) == File.separatorChar)
        {
            return baseDir + fileDir;
        }
        else
        {
            return baseDir + File.separatorChar + fileDir;
        }
    }

    public static int firstRoundWeightAnalysis(TreeMap<Integer, Integer> mapAdnWeight)
    {
        /* 根据最大优先级 和 可选排期列表, 构造权重散列Map */
        int weightTotalValue = 0;
        for (Integer platId : mapAdnWeight.keySet())
        {
            weightTotalValue += mapAdnWeight.get(platId);
        }

        if (weightTotalValue <= 0 || mapAdnWeight.isEmpty())
        {
            log.error("weightAnalysis: mapAdnWeight(" + mapAdnWeight.toString() + ") invalid");
            return OssConstants.RET_ERROR;
        }

        /* mapAdnWeight中存储了优先级最高的排期信息: key是平台id, value是权重值 */
        int randomValue = new Random().nextInt(weightTotalValue);
        randomValue++;

        int selectAdnPlatId = OssConstants.RET_ERROR;
        Set<Integer> keys = mapAdnWeight.keySet();
        for (Integer key : keys)
        {
            int value = mapAdnWeight.get(key);
            if (randomValue <= value)
            {
                selectAdnPlatId = key;
                break;
            }
            else
            {
                randomValue = randomValue - value;
            }
        }

        if (OssConstants.RET_ERROR == selectAdnPlatId)
        {
            log.error("select adn plat-id(" + selectAdnPlatId + ")'s value invalid");
            return OssConstants.RET_ERROR;
        }

        return selectAdnPlatId;
    }

    public static class DataConvert
    {
        public static int toInt(Object objValue, int defValue)
        {
            int intValue = defValue;
            if (null == objValue)
            {
                log.debug("convert null to int-value error, and set def-value=" + defValue);
                return defValue;
            }

            if (objValue instanceof Boolean)
            {
                intValue = ((Boolean) objValue) ? 1 : 0;
            }
            else
            {
                String strValue = objValue.toString().trim();
                if (strValue.length() <= 0)
                {
                    return defValue;
                }

                try
                {
                    intValue = Integer.parseInt(strValue);
                }
                catch (Exception e)
                {
                    log.error("convert " + strValue + " to int-value error, and set def-value=" + defValue);
                    intValue = defValue;
                }
            }

            return intValue;
        }

        public static long toLong(Object objValue, long defValue)
        {
            long longValue = defValue;
            if (null == objValue)
            {
                log.debug("convert null to long-value error, and set def-value=" + defValue);
                return defValue;
            }

            if (objValue instanceof Boolean)
            {
                longValue = ((Boolean) objValue) ? 1 : 0;
            }
            else
            {
                String strValue = objValue.toString().trim();
                if (strValue.length() <= 0)
                {
                    return defValue;
                }

                try
                {
                    longValue = Long.parseLong(strValue);
                }
                catch (Exception e)
                {
                    log.error("convert " + strValue + " to long-value error, and set def-value=" + defValue);
                    longValue = defValue;
                }
            }

            return longValue;
        }

        public static float toFloat(Object objValue, float defValue)
        {
            float floatValue = defValue;
            if (null == objValue)
            {
                log.debug("convert null to float-value error, and set def-value=" + defValue);
                return defValue;
            }

            if (objValue instanceof Boolean)
            {
                floatValue = ((Boolean) objValue) ? 1 : 0;
            }
            else
            {
                String strValue = objValue.toString().trim();
                if (strValue.length() <= 0)
                {
                    return defValue;
                }

                try
                {
                    floatValue = Float.parseFloat(strValue);
                }
                catch (Exception e)
                {
                    log.error("convert " + strValue + " to float-value error, and set def-value=" + defValue);
                    floatValue = defValue;
                }
            }

            return floatValue;
        }

        public static Double toDouble(Object objValue, double defValue)
        {
            double doubleValue = defValue;
            if (null == objValue)
            {
                log.debug("convert null to double-value error, and set def-value=" + defValue);
                return defValue;
            }

            if (objValue instanceof Boolean)
            {
                doubleValue = ((Boolean) objValue) ? 1 : 0;
            }
            else
            {
                String strValue = objValue.toString().trim();
                if (strValue.length() <= 0)
                {
                    return defValue;
                }

                try
                {
                    doubleValue = Double.parseDouble(strValue);
                }
                catch (Exception e)
                {
                    log.error("convert " + strValue + " to doubleValue-value error, and set def-value=" + defValue);
                    doubleValue = defValue;
                }
            }

            return doubleValue;
        }

        public static BigDecimal toBigDecimal(Object objValue, long defValue)
        {
            long longValue = defValue;
            if (null == objValue)
            {
                log.debug("convert null to long-value error, and set def-value=" + defValue);
                return BigDecimal.valueOf(defValue);
            }

            if (objValue instanceof BigDecimal)
            {
                return (BigDecimal) objValue;
            }
            else
            {
                String strValue = objValue.toString().trim();
                if (strValue.length() <= 0)
                {
                    return BigDecimal.valueOf(defValue);
                }

                try
                {
                    Double dValue = Double.parseDouble(strValue);
                    return BigDecimal.valueOf(dValue);
                }
                catch (Exception e)
                {
                    log.error("convert " + strValue + " to long-value error, and set def-value=" + defValue);
                    return BigDecimal.valueOf(defValue);
                }
            }
        }

        public static String toStr(Object objValue, String defValue)
        {
            if (null == objValue)
            {
                log.debug("convert null to string-value error, and set def-value=" + defValue);
                return defValue;
            }

            return objValue.toString();
        }

        public static Set<Integer> toSetInt(Object objValue)
        {
            Set<Integer> setIntValue = Sets.newHashSet();
            if (null == objValue)
            {
                return setIntValue;
            }

            /* 如果是空字符串, 则返回空; 否则外层认为有一个有效的空值, 这TM扯蛋 */
            String strValues = objValue.toString().trim();
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

        public static String toUpperStr(Object objValue, String defValue)
        {
            if (null == objValue)
            {
                log.debug("convert null to string-value error, and set def-value=" + defValue);

                if (null == defValue)
                {
                    return defValue;
                }
                else
                {
                    return defValue.toUpperCase();
                }
            }

            return objValue.toString().toUpperCase();
        }
    }
}
