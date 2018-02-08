package cn.zyy.oss.share;

import java.util.Map;

import com.google.common.collect.Maps;

public class OssRequest
{
    public int                 type     = -1;               /* 0-GET, 1-POST */
    public String              url      = null;
    public String              getQuery;
    public Map<String, Object> headers  = Maps.newHashMap();
    public Map<String, String> getParam = Maps.newHashMap();
    public byte[]              postBody = null;
    public Object              srvInfo  = null;

    public String toString()
    {
        StringBuilder strSB = new StringBuilder();
        strSB.append("\trequest-type: " + ((0 == type) ? "GET" : "POST") + "\n");
        strSB.append("\turl: " + url + "\n");
        strSB.append("\tget_query: " + getQuery + "\n");
        strSB.append("\tpost_body: " + ((null == postBody) ? "null" : (new String(postBody))) + "\n");
        strSB.append("\theaders: " + headers.toString() + "\n");
        strSB.append("\tsrvInfo: " + srvInfo.toString() + "\n");

        return strSB.toString();
    }

    public String getCltIp()
    {
        if (null == headers)
        {
            return null;
        }

        Object objValue = headers.get(OssConstants.HTTP_HEAD_CLT_IP_FIELD);
        if (null == objValue)
        {
            return null;
        }
        String strIps = objValue.toString();

        if (!strIps.contains(","))
        {
            return strIps;
        }

        String[] aIp = strIps.split(",");
        return aIp[aIp.length - 1].trim();
    }

    public OssRequest copyRequest()
    {
        OssRequest copyHttp = new OssRequest();
        copyHttp.type = type;
        copyHttp.url = url;
        copyHttp.getQuery = getQuery;

        if (null != postBody && postBody.length > 0)
        {
            copyHttp.postBody = new byte[postBody.length];
            System.arraycopy(postBody, 0, copyHttp.postBody, 0, postBody.length);
        }

        for (String tmpKey : getParam.keySet())
        {
            copyHttp.getParam.put(tmpKey, getParam.get(tmpKey));
        }

        for (String tmpKey : headers.keySet())
        {
            copyHttp.headers.put(tmpKey, headers.get(tmpKey).toString());
        }

        return copyHttp;
    }
}
