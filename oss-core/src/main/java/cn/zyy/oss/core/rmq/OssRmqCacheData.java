package cn.zyy.oss.core.rmq;

import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.google.common.collect.Maps;

import cn.zyy.oss.share.OssCacheData;
import cn.zyy.oss.share.OssLog;

public class OssRmqCacheData extends OssCacheData
{
    private static final OssLog log               = new OssLog(OssLog.LOG_MODULE_OSS);

    private static final String DELIMETER         = " ";
    private static final String DELIMETER_LEVEL_2 = ",";
    private static final String DELIMETER_LEVEL_3 = "=";

    public String               key;
    public byte[]               msg;
    public Map<String, String>  userProperty      = Maps.newHashMap();

    public OssRmqCacheData()
    {
        super();
        this.key = null;
        this.msg = null;
    }

    public OssRmqCacheData(String key, byte[] byteMsg, Map<String, String> userProperty)
    {
        super();
        this.key = key;
        this.msg = byteMsg;
        this.userProperty = userProperty;
    }

    @Override
    public String serialString()
    {
        /* 序列化userProperty为: A=B,C=D */
        String strProperty = "";
        if (null != userProperty)
        {
            for (String pKey : userProperty.keySet())
            {
                String pValue = userProperty.get(pKey);
                if (null == pValue || pValue.trim().length() <= 0)
                {
                    continue;
                }

                if (strProperty.length() > 0)
                {
                    strProperty += DELIMETER_LEVEL_2;
                }

                strProperty += (pKey.trim() + DELIMETER_LEVEL_3 + pValue.trim());
            }
        }

        if (strProperty.length() <= 0)
        {
            return key + DELIMETER + Base64.encodeBase64String(msg);
        }
        else
        {
            return key + DELIMETER + strProperty + DELIMETER + Base64.encodeBase64String(msg);
        }
    }

    @Override
    public OssCacheData deserialFromString(String strFormat)
    {
        int firstDelimPos = strFormat.indexOf(DELIMETER);
        int lastDelimPos = strFormat.lastIndexOf(DELIMETER);
        if (firstDelimPos < 0)
        {
            return null;
        }

        key = strFormat.substring(0, firstDelimPos);

        String strData = strFormat.substring(lastDelimPos + 1);
        msg = Base64.decodeBase64(strData);

        if (firstDelimPos < lastDelimPos)
        {
            String strProperty = strFormat.substring(firstDelimPos + 1, lastDelimPos).trim();
            if (null != strProperty)
            {
                String[] aKeyValue = strProperty.split(DELIMETER_LEVEL_2);
                if (null != aKeyValue)
                {
                    for (String tmpKeyValue : aKeyValue)
                    {
                        String[] aField = tmpKeyValue.split(DELIMETER_LEVEL_3);
                        if (null != aField && 2 == aField.length)
                        {
                            userProperty.put(aField[0].trim(), aField[1].trim());
                        }
                        else
                        {
                            log.error("invalid cache producer-rmq record, parse property[" + strProperty + "] error");
                            return null;
                        }
                    }
                }
                else
                {
                    log.error("invalid cache producer-rmq record, parse property[" + strProperty + "] error");
                    return null;
                }
            }
        }

        return this;
    }
}
