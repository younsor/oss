package com.oss.http.server;

import static org.httpkit.HttpUtils.HttpEncode;

import java.nio.ByteBuffer;

import org.httpkit.HeaderMap;
import org.httpkit.server.AsyncChannel;
import org.httpkit.server.Frame;
import org.httpkit.server.HttpRequest;
import org.httpkit.server.IHandler;
import org.httpkit.server.RespCallback;

import cn.zyy.oss.http.OssServer;
import cn.zyy.oss.share.OssLog;

public class MTestServer extends OssServer
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    public static class GTestServerHandler implements IHandler
    {
        public void close(int timeoutMs)
        {
            log.debug("close.");
        }

        public void handle(AsyncChannel channel, Frame frame)
        {
            log.error("handle channel and frame: not support");
        }

        public void handle(HttpRequest request, final RespCallback callback)
        {}

        public void handle(AsyncChannel channel, Frame.TextFrame frame)
        {
            log.debug("handle channel and TextFrame frame");
        }

        public void clientClose(AsyncChannel channel, int status)
        {
            log.debug("handle channel and status: client has closed the channel.");
        }
    }

    public MTestServer(String ip, int port, IHandler handler, int nioThreadNum)
    {
        super(ip, port, handler, nioThreadNum);
    }

    public static void sendRsponse(RespCallback callback, byte[] rspBytes)
    {
        ByteBuffer[] bytes = null;
        HeaderMap header = new HeaderMap();
        header.put("Connection", "Keep-Alive");
        header.put("Content-Type", "application/octet-stream");

        if (null == rspBytes)
        {
            bytes = HttpEncode(204, header, "");
        }
        else
        {
            bytes = HttpEncode(200, header, rspBytes);
        }

        callback.run(bytes);
    }

    public static void main(String[] args) throws Exception
    {
        if (null == args || args.length < 2)
        {
            System.err.println("null == args || args.length < 2");
            return;
        }

        int taskNum = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        MTestServer server = new MTestServer("0.0.0.0", port, new GTestServerHandler(), taskNum);
        if (!server.start())
        {
            System.err.println("server.start error");
            return;
        }

        while (true)
        {
            Thread.sleep(2000);
        }
    }
}
