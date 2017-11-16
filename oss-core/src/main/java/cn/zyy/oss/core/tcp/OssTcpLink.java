package cn.zyy.oss.core.tcp;

import java.net.Socket;

import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpLink
{
    private static final OssLog log      = new OssLog(OssLog.LOG_MODULE_OSS);

    /* 当前链路是否可用 */
    public boolean              isUse    = false;

    /* 本端在该链路的角色: true-客户端角色, false-服务端角色 */
    public boolean              isClt    = false;

    public OssTcpSend           sendTask = null;
    public OssTcpRecv           recvTask = null;
    public Socket               socket   = null;

    public String toString()
    {
        String linkId = "client";
        if (!isClt)
        {
            linkId = "server";
        }

        String localAddress = socket.getLocalAddress().toString();
        int localPort = socket.getLocalPort();

        String endAddress = socket.getInetAddress().toString();
        int endPort = socket.getPort();

        int recvBuffLenKB = 0;
        int sendBuffLenKB = 0;
        try
        {
            recvBuffLenKB = socket.getReceiveBufferSize() / 1024;
            sendBuffLenKB = socket.getSendBufferSize() / 1024;
        }
        catch (Exception e)
        {
            log.error("get socket buff-size exception\n" + OssFunc.getExceptionInfo(e));
        }

        int sendTno = 0;
        if (null != sendTask)
        {
            sendTno = sendTask.getTno();
        }

        int recvTno = 0;
        if (null != recvTask)
        {
            recvTno = recvTask.getTno();
        }

        return String.format("socket[%s] local[%s-%d] end[%s-%d] sbuf=%dKB rbuf=%dKB s-tno=%d r-tno=%d", linkId, localAddress, localPort, endAddress, endPort, sendBuffLenKB, recvBuffLenKB, sendTno, recvTno);
    }
}
