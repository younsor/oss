package cn.zyy.oss.share;

import java.util.Date;
import java.util.Map;

import com.google.common.collect.Maps;

public class OssFreqRecord extends OssCacheData
{
    private static final OssLog log                         = new OssLog(OssLog.LOG_MODULE_OSS);

    private static final String KEY_VALUE_DELIMETER         = " , ";
    private static final String FIELD_DELIMETER             = "-";
    private static final String VALUE_DELIMETER             = "|";
    private static final String VALUE_SCHE_DELIMETER        = ":";

    private static final String KEY_VALUE_SPLITE_DELIMETER  = " , ";
    private static final String FIELD_SPLITE_DELIMETER      = "-";
    private static final String VALUE_SPLITE_DELIMETER      = "\\|";
    private static final String VALUE_SCHE_SPLITE_DELIMETER = ":";

    public static String formatTime(Date thatDate)
    {
        return String.format("%04d%02d%02d%02d", thatDate.getYear() + 1900, thatDate.getMonth() + 1, thatDate.getDate(), thatDate.getHours());
    }

    public class RecordValue
    {
        public String time;
        public int    hourImpNum;
        public int    dayImpNum;
        public int    fullImpNum;
        public int    hourClkNum;
        public int    dayClkNum;
        public int    fullClkNum;

        public int compareTimeDay(Date thatDate)
        {
            String day = time.substring(0, 8);
            String thatDay = String.format("%04d%02d%02d", thatDate.getYear() + 1900, thatDate.getMonth() + 1, thatDate.getDate());
            return day.compareTo(thatDay);
        }

        public int compareTimeHour(Date thatDate)
        {
            String thatHour = String.format("%04d%02d%02d%02d", thatDate.getYear() + 1900, thatDate.getMonth() + 1, thatDate.getDate(), thatDate.getHours());
            return time.compareTo(thatHour);
        }

        public String toString()
        {
            StringBuilder strBuff = new StringBuilder();

            /* 如果有多条记录值, 则用|分割 */
            if (strBuff.length() > 0)
            {
                strBuff.append(VALUE_DELIMETER);
            }

            /* 时间信息 */
            strBuff.append(time);
            strBuff.append(FIELD_DELIMETER);

            /* 频次数量信息 */
            strBuff.append(hourImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(dayImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(fullImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(hourClkNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(dayClkNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(fullClkNum);

            return strBuff.toString();
        }
    }

    /* key信息 */
    public String                   did;
    public Map<String, RecordValue> mapValue = Maps.newTreeMap();

    public OssFreqRecord(Date tsDate, String did, String orderId, int impNum, int clkNum)
    {
        this.did = did;

        RecordValue freqValue = new RecordValue();
        freqValue.time = formatTime(tsDate);
        freqValue.hourImpNum = impNum;
        freqValue.dayImpNum = impNum;
        freqValue.fullImpNum = impNum;
        freqValue.hourClkNum = clkNum;
        freqValue.dayClkNum = clkNum;
        freqValue.fullClkNum = clkNum;

        this.mapValue.put(orderId, freqValue);
    }

    /** 频次信息格式如下:  
     * 一条频次信息结构如下：
     * key为：did
     * value为：orderId:hourTime-hourImpNum-dayImpNum-fullImpNum-hourClkNum-dayClkNum-fullClkNum
     * 
     * 一个用户的频次信息结构为：
     * key [value|value|...]
     * 
     * */
    public OssFreqRecord(String freqKey, String freqValue) throws Exception
    {
        parse(freqKey, freqValue);
    }

    public OssFreqRecord(String keyValue) throws Exception
    {
        String[] aKeyValue = keyValue.split(KEY_VALUE_SPLITE_DELIMETER);
        if (null == aKeyValue || 2 != aKeyValue.length)
        {
            throw new Exception("parse freq-value exception: " + keyValue);
        }

        String freqKey = aKeyValue[0].trim();
        String freqValue = aKeyValue[1].trim();

        parse(freqKey, freqValue);
    }

    /** 调用者需要保证key是一样的 */
    private OssFreqRecord mergeOneValue(String valueKey, RecordValue valueInfo)
    {
        RecordValue mergeValueInfo = mapValue.get(valueKey);
        if (null == mergeValueInfo)
        {
            mapValue.put(valueKey, valueInfo);
        }
        else
        {
            /* 合并mergeValueInfo 与 valueInfo, 将结果保存到mergeValueInfo中 */
            mergeValueInfo.fullImpNum += valueInfo.fullImpNum;
            mergeValueInfo.fullClkNum += valueInfo.fullClkNum;

            String mergeDay = mergeValueInfo.time.substring(0, 8);
            String day = valueInfo.time.substring(0, 8);
            int compareDay = mergeDay.compareTo(day);
            if (compareDay > 0)
            {
                /* 天、小时都大, 则天量、小时量、时间均以mergeValueInfo为准 */
            }
            else if (compareDay < 0)
            {
                /* 天、小时都小, 则天量、小时量、时间均以valueInfo为准 */
                mergeValueInfo.time = valueInfo.time;
                mergeValueInfo.hourImpNum = valueInfo.hourImpNum;
                mergeValueInfo.hourClkNum = valueInfo.hourClkNum;

                mergeValueInfo.dayImpNum = valueInfo.dayImpNum;
                mergeValueInfo.dayClkNum = valueInfo.dayClkNum;
            }
            else
            {
                /* 天一样, 则看小时 */
                mergeValueInfo.dayImpNum += valueInfo.dayImpNum;
                mergeValueInfo.dayClkNum += valueInfo.dayClkNum;

                String mergeHour = mergeValueInfo.time.substring(8);
                String hour = valueInfo.time.substring(8);
                int compareHour = mergeHour.compareTo(hour);

                if (compareHour > 0)
                {
                    /* 小时大, 小时量、时间均以mergeValueInfo为准 */
                }
                else if (compareHour < 0)
                {
                    /* 小时小, 则小时量、时间均以valueInfo为准 */
                    mergeValueInfo.time = valueInfo.time;
                    mergeValueInfo.hourImpNum = valueInfo.hourImpNum;
                    mergeValueInfo.hourClkNum = valueInfo.hourClkNum;
                }
                else
                {
                    /* 小时一样, 时间不变, 小时量相加 */
                    mergeValueInfo.hourImpNum += valueInfo.hourImpNum;
                    mergeValueInfo.hourClkNum += valueInfo.hourClkNum;
                }
            }
        }

        return this;
    }

    private void parse(String freqKey, String freqValue) throws Exception
    {
        String[] aValueRecord = freqValue.split(VALUE_SPLITE_DELIMETER);
        if (null == aValueRecord || aValueRecord.length <= 0)
        {
            throw new Exception("parse freq-value exception: " + freqValue);
        }

        /* 解析key信息 */
        this.did = freqKey;

        /* 解析value信息 */
        for (String strValue : aValueRecord)
        {
            String[] aStrTmp = strValue.split(VALUE_SCHE_SPLITE_DELIMETER);
            if (null == aStrTmp || aStrTmp.length != 2)
            {
                throw new Exception("parse freq-value exception: " + freqValue);
            }

            /* 排期信息 */
            String scheId = aStrTmp[0];

            /* 该用户在该排期下的频次信息 */
            RecordValue valueRecord = new RecordValue();
            String[] aField = aStrTmp[1].split(FIELD_SPLITE_DELIMETER);
            if (null == aField || aField.length != 7)
            {
                throw new Exception("parse freq-value exception: " + freqValue);
            }

            valueRecord.time = aField[0];
            valueRecord.hourImpNum = Integer.parseInt(aField[1]);
            valueRecord.dayImpNum = Integer.parseInt(aField[2]);
            valueRecord.fullImpNum = Integer.parseInt(aField[3]);
            valueRecord.hourClkNum = Integer.parseInt(aField[4]);
            valueRecord.dayClkNum = Integer.parseInt(aField[5]);

            /** bug: 之前用的freq-value的分隔符是"\\|", 多了一个'\'字符
             *  "1024:2017021817-3-7-7-1-1-7\" 
             *  */
            if (aField[6].charAt(aField[6].length() - 1) == '\\')
            {
                valueRecord.fullClkNum = Integer.parseInt(aField[6].substring(0, aField[6].length() - 1));
            }
            else
            {
                valueRecord.fullClkNum = Integer.parseInt(aField[6]);
            }

            mergeOneValue(scheId, valueRecord);
        }
    }

    public String formatKey()
    {
        return did;
    }

    public String formatValue()
    {
        StringBuilder strBuff = new StringBuilder();

        for (String scheId : mapValue.keySet())
        {
            RecordValue oneFreqValue = mapValue.get(scheId);

            /* 如果有多条记录值, 则用|分割 */
            if (strBuff.length() > 0)
            {
                strBuff.append(VALUE_DELIMETER);
            }

            /* 排期信息 */
            strBuff.append(scheId);
            strBuff.append(VALUE_SCHE_DELIMETER);

            /* 时间信息 */
            strBuff.append(oneFreqValue.time);
            strBuff.append(FIELD_DELIMETER);

            /* 频次数量信息 */
            strBuff.append(oneFreqValue.hourImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(oneFreqValue.dayImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(oneFreqValue.fullImpNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(oneFreqValue.hourClkNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(oneFreqValue.dayClkNum);
            strBuff.append(FIELD_DELIMETER);
            strBuff.append(oneFreqValue.fullClkNum);
        }

        return strBuff.toString();
    }

    /** 调用者需要保证key是一样的 */
    public OssFreqRecord merge(OssFreqRecord that)
    {
        for (String orderId : that.mapValue.keySet())
        {
            RecordValue oneFreqValue = that.mapValue.get(orderId);

            mergeOneValue(orderId, oneFreqValue);
        }

        return this;
    }

    public String toString()
    {
        return "key[" + formatKey() + "] value[" + formatValue() + "]";
    }

    @Override
    public String serialString()
    {
        return formatKey() + KEY_VALUE_DELIMETER + formatValue();
    }

    @Override
    public OssCacheData deserialFromString(String keyValue)
    {
        String[] aKeyValue = keyValue.split(KEY_VALUE_SPLITE_DELIMETER);
        if (null == aKeyValue || 2 != aKeyValue.length)
        {
            return null;
        }

        String freqKey = aKeyValue[0].trim();
        String freqValue = aKeyValue[1].trim();

        try
        {
            parse(freqKey, freqValue);
        }
        catch (Exception e)
        {
            log.error("parse exception: " + keyValue + "\n" + OssFunc.getExceptionInfo(e));
            return null;
        }

        return this;
    }
}
