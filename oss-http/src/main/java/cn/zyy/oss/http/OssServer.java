package cn.zyy.oss.http;

import java.io.IOException;

import org.httpkit.server.HttpServer;
import org.httpkit.server.IHandler;

import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssServer
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    private String              ip;
    private int                 port;
    private IHandler            handler;
    private int                 nioThreadNum;
    private HttpServer          httpServer;

    public OssServer(String ip, int port, IHandler handler)
    {
        this(ip, port, handler, 1);
    }

    public OssServer(String ip, int port, IHandler handler, int nioThreadNum)
    {
        this.ip = ip;
        this.port = port;
        this.handler = handler;
        this.nioThreadNum = nioThreadNum;
    }

    public boolean start()
    {
        try
        {
            httpServer = new HttpServer(this.ip, this.port, this.handler, 20480, 2048, 1024 * 1024 * 4, nioThreadNum);
        }
        catch (IOException e)
        {
            httpServer = null;

            log.error("Http Server(" + ip + ":" + port + ") bind fail, exception: \n" + OssFunc.getExceptionInfo(e));
            return false;
        }

        log.info("Http Server(" + ip + ":" + port + ") bind success!");
        try
        {
            httpServer.start();
        }
        catch (IOException e)
        {
            httpServer.stop(2);
            httpServer = null;

            log.error("Http Server(" + ip + ":" + port + ") start fail, exception: \n" + OssFunc.getExceptionInfo(e));
            return false;
        }

        return true;
    }

    public void close()
    {
        log.info("Http Server(" + ip + ":" + port + ") close!");
        try
        {
            /* 关闭链路的时候等100ms */
            httpServer.stop(100);
        }
        catch (Exception e)
        {
            log.error("Http Server(" + ip + ":" + port + ") close fail, exception: \n" + OssFunc.getExceptionInfo(e));
        }
    }
}
