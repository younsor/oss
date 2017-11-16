package org.httpkit.server;

import static org.httpkit.HttpUtils.CHARSET;
import static org.httpkit.HttpUtils.CONNECTION;
import static org.httpkit.HttpUtils.CONTENT_TYPE;
import static org.httpkit.HttpUtils.getStringValue;
import static org.httpkit.HttpVersion.HTTP_1_1;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.httpkit.BytesInputStream;
import org.httpkit.HttpMethod;
import org.httpkit.HttpUtils;
import org.httpkit.HttpVersion;

import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssLog;
import cn.zyy.oss.share.OssRequest;

public class HttpRequest
{
    private static final OssLog log           = new OssLog(OssLog.LOG_MODULE_OSS);

    public final String         queryString;
    public final String         uri;
    public final HttpMethod     method;
    public final HttpVersion    version;

    private byte[]              body;

    // package visible
    int                         serverPort    = 80;
    String                      serverName;
    Map<String, Object>         headers;
    int                         contentLength = 0;
    String                      contentType;
    String                      charset       = "utf8";
    boolean                     isKeepAlive   = false;
    boolean                     isWebSocket   = false;

    InetSocketAddress           remoteAddr;
    AsyncChannel                channel;

    public HttpRequest(HttpMethod method, String url, HttpVersion version)
    {
        this.method = method;
        this.version = version;
        int idx = url.indexOf('?');
        if (idx > 0)
        {
            uri = url.substring(0, idx);
            queryString = url.substring(idx + 1);
        }
        else
        {
            uri = url;
            queryString = null;
        }
    }

    public InputStream getBody()
    {
        if (body != null)
        {
            return new BytesInputStream(body, contentLength);
        }
        return null;
    }

    public String getRemoteAddr()
    {
        String h = getStringValue(headers, HttpUtils.X_FORWARDED_FOR);
        if (null != h)
        {
            int idx = h.indexOf(',');
            if (idx == -1)
            {
                return h;
            }
            else
            {
                // X-Forwarded-For: client, proxy1, proxy2
                return h.substring(0, idx);
            }
        }
        else
        {
            return remoteAddr.getAddress().getHostAddress();
        }
    }

    public void setBody(byte[] body, int count)
    {
        this.body = body;
        this.contentLength = count;
    }

    public void setHeaders(Map<String, Object> headers)
    {
        String h = getStringValue(headers, "host");
        if (h != null)
        {
            int idx = h.lastIndexOf(':');
            if (idx != -1)
            {
                this.serverName = h.substring(0, idx);
                serverPort = Integer.valueOf(h.substring(idx + 1));
            }
            else
            {
                this.serverName = h;
            }
        }

        String ct = getStringValue(headers, CONTENT_TYPE);
        if (ct != null)
        {
            int idx = ct.indexOf(";");
            if (idx != -1)
            {
                int cidx = ct.indexOf(CHARSET, idx);
                if (cidx != -1)
                {
                    contentType = ct.substring(0, idx);
                    charset = ct.substring(cidx + CHARSET.length());
                }
                else
                {
                    contentType = ct;
                }
            }
            else
            {
                contentType = ct;
            }
        }

        String con = getStringValue(headers, CONNECTION);
        if (con != null)
        {
            con = con.toLowerCase();
        }

        isKeepAlive = (version == HTTP_1_1 && !"close".equals(con)) || "keep-alive".equals(con);
        isWebSocket = "websocket".equalsIgnoreCase(getStringValue(headers, "upgrade"));
        this.headers = headers;
    }

    public byte[] getPostBody()
    {
        return body;
    }

    public Map<String, Object> getHeaders()
    {
        Map<String, Object> mapHeaders = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : headers.entrySet())
        {
            mapHeaders.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        return mapHeaders;
    }

    /* 将HttpRequest转换为OssRequest */
    public static OssRequest convert2OssRequest(HttpRequest request)
    {
        OssRequest originReq = new OssRequest();
        originReq.url = request.uri.toLowerCase().trim();
        originReq.getQuery = request.queryString;
        originReq.headers = request.getHeaders();

        if (request.method.KEY == request.method.GET.KEY)
        {
            originReq.type = OssConstants.HTTP_REQ_TYPE_GET;
            if (null == request.queryString || request.queryString.length() <= 0)
            {
                /* 但是不算错误 */
                log.error("GET-request's queryString is empty, url=" + originReq.url);
            }
            else
            {
                String[] strGetParam = request.queryString.split("&");
                if (null != strGetParam && strGetParam.length > 0)
                {
                    for (String oneParam : strGetParam)
                    {
                        /* 找到第一个"="号位置 */
                        int pos = oneParam.indexOf("=");
                        if (pos <= 0)
                        {
                            log.error("invalid getParam in FET-request when queryString=" + request.queryString);
                            continue;
                        }

                        String key = oneParam.substring(0, pos);
                        if (null == key)
                        {
                            log.error("invalid getParam in FET-request when queryString=" + request.queryString);
                            continue;
                        }

                        String value = oneParam.substring(pos + 1);
                        originReq.getParam.put(key.trim().toLowerCase(), ((null == value) ? "" : value.trim()));
                    }
                }
            }
        }
        else if (request.method.KEY == request.method.POST.KEY)
        {
            originReq.type = OssConstants.HTTP_REQ_TYPE_POST;
            if (null == request.body || request.body.length <= 0)
            {
                /* 但是不算错误 */
                log.error("POST-request's body is empty, url=" + originReq.url);
            }
            else
            {
                originReq.postBody = request.getPostBody();
            }
        }
        else
        {
            log.error("http-handle: handler req error, request's type=" + request.method.KEY.toString());
            return null;
        }

        return originReq;
    }
}
