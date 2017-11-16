package cn.zyy.oss.monserver.srv;

import static org.httpkit.HttpUtils.HttpEncode;

import java.io.IOException;

import org.httpkit.HeaderMap;
import org.httpkit.server.AsyncChannel;
import org.httpkit.server.Frame;
import org.httpkit.server.HttpRequest;
import org.httpkit.server.IHandler;
import org.httpkit.server.RespCallback;

import cn.zyy.oss.core.proto.SysMonitor;
import cn.zyy.oss.monserver.main.MMonServer;
import cn.zyy.oss.monserver.share.MConstants;
import cn.zyy.oss.monserver.share.MSessInfo;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;
import cn.zyy.oss.share.OssRequest;

public class GMonServerHandler implements IHandler
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    public void close(int timeoutMs)
    {
        log.debug("close.");
    }

    public void handle(AsyncChannel channel, Frame frame)
    {
        log.error("handle channel and frame: not support");
    }

    private void rspxxx(RespCallback callback, int code)
    {
        HeaderMap header = new HeaderMap();
        header.put("Connection", "Keep-Alive");
        header.put("Content-Type", "application/octet-stream");
        callback.run(HttpEncode(code, header, ""));
    }

    public void handle(HttpRequest request, final RespCallback callback)
    {
        /* 异常检查 */
        if (null == request.uri)
        {
            log.error("invalid request: " + request.toString());
            rspxxx(callback, 404);
            return;
        }

        /* 保活响应 */
        if ("/alive".equals(request.uri))
        {
            log.debug("recv alive-msg, and response 200-OK");
            rspxxx(callback, 200);
            return;
        }

        /* 将底层的请求 转换为 OssRequest */
        OssRequest originReq = HttpRequest.convert2OssRequest(request);
        if (null == originReq)
        {
            rspxxx(callback, 403);
            return;
        }

        if (!originReq.url.equals("/monitor"))
        {
            log.error("originReq.url[%s] not /monitor", originReq.url);
            return;
        }

        SysMonitor.Info monitorInfo = null;
        try
        {
            monitorInfo = SysMonitor.Info.parseFrom(request.getBody());
        }
        catch (IOException e)
        {
            monitorInfo = null;
            log.error(OssFunc.getExceptionInfo(e));
        }

        if (null == monitorInfo)
        {
            HeaderMap header = new HeaderMap();
            header.put("Connection", "Keep-Alive");
            callback.run(HttpEncode(400, header, ""));

            log.error("recv mon-msg, parse error, and return 400.");
            return;
        }

        MSessInfo sessInfo = new MSessInfo();
        sessInfo.monInfo = monitorInfo;
        sessInfo.callback = callback;

        MMonServer.getSrvModule().sendMsgEx(MConstants.MSG_ID_SRV_RECV_MON, sessInfo, MConstants.TASK_NO_SRV, MConstants.TASK_NO_SRV);
    }

    public void handle(AsyncChannel channel, Frame.TextFrame frame)
    {
        log.debug("handle channel and TextFrame frame");
    }

    public void clientClose(AsyncChannel channel, int status)
    {
        log.debug("handle channel and status: client has closed the channel.");
    }
}
