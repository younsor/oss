package cn.zyy.oss.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.httpkit.BytesInputStream;
import org.httpkit.HttpMethod;
import org.httpkit.client.HttpClient;
import org.httpkit.client.IFilter;
import org.httpkit.client.IResponseHandler;
import org.httpkit.client.RequestConfig;
import org.httpkit.client.RespListener;

import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;
import cn.zyy.oss.share.OssRequest;

public class OssClient
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);
    
    public static String getBodyString(Object body)
    {
        String strBody = null;
        if (body instanceof String)
        {
            strBody = (String) body;
        }
        else if (body instanceof BytesInputStream)
        {
            byte[] bytes = ((org.httpkit.BytesInputStream) body).bytes();
            strBody = new String(bytes);
        }
        else if (null != body)
        {
            log.error("recv rsp's body's class[" + body.getClass().getName() + "] invalid");
            return null;
        }

        return strBody;
    }

    private String          clientName;
    private HttpClient      httpClient;
    private ExecutorService pool = null;

    public OssClient(String clientName)
    {
        this.clientName = clientName;
        pool = Executors.newCachedThreadPool();
    }

    public boolean start()
    {
        try
        {
            httpClient = new HttpClient(clientName);
        }
        catch (IOException e)
        {
            OssFunc.getExceptionInfo(e);
            return false;
        }

        return true;
    }

    public void stop()
    {
        if (null != httpClient)
        {
            try
            {
                httpClient.stop();
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                log.error(OssFunc.getExceptionInfo(e));
            }
        }
    }

    public void sendRequest(OssRequest reqInfo, IResponseHandler rspHandler, int timeout)
    {
        RequestConfig reqConfig = null;
        String url = reqInfo.url;
        if (OssConstants.HTTP_REQ_TYPE_GET == reqInfo.type)
        {
            reqConfig = new RequestConfig(HttpMethod.GET, reqInfo.headers, null, timeout, 5000);
        }
        else
        {
            ByteBuffer byteBuff = ByteBuffer.wrap(reqInfo.postBody);
            reqConfig = new RequestConfig(HttpMethod.POST, reqInfo.headers, byteBuff, timeout, 5000);
        }

        httpClient.exec(url, reqConfig, null, new RespListener(rspHandler, IFilter.ACCEPT_ALL, pool, 1));
    }

    public String getName()
    {
        return clientName;
    }
}
