package cn.zyy.oss.core.tcp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskPdc;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpAccept extends OssTaskPdc
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    private boolean             isOK;
    private ServerSocket        srvSocket;
    private OssTcpServer        srvMgrTask;

    protected OssTcpAccept(OssTcpServer serverTask, ServerSocket recvSock, String taskName, int iPriority)
    {
        super(OssCoreConstants.TNO_INVALID, taskName, iPriority);

        srvSocket = recvSock;

        /* 链路初始化是OK的 */
        isOK = true;

        srvMgrTask = serverTask;

        start();
    }

    public boolean isAcceptOK()
    {
        return isOK;
    }

    /* 由管理任务调用 */
    public void closeSocket()
    {
        if (null == srvSocket)
        {
            return;
        }

        try
        {
            srvSocket.close();
        }
        catch (Exception e)
        {
            log.error("close server-socket exception\n" + OssFunc.getExceptionInfo(e));
        }

        srvSocket = null;
    }

    @Override
    public void onEntry()
    {
        while (true)
        {
            if (null == srvSocket)
            {
                isOK = false;
                break;
            }

            Socket socket = null;
            try
            {
                socket = srvSocket.accept(); // 从连接请求队列中取出一个连接
            }
            catch (Exception e)
            {
                log.error("server-socket accept exception\n" + OssFunc.getExceptionInfo(e));

                socket = null;
            }

            /* 服务监听套接字是否有异常 */
            if (srvSocket.isClosed() || (!srvSocket.isBound()))
            {
                isOK = false;

                closeSocket();
                break;
            }

            /* 如果socket有效, 给Tcp服务端管理任务发送"接收链路消息" */
            if (OssCoreConstants.RET_OK != srvMgrTask.sendMsg(OssCoreConstants.MSG_ID_TCP_ACCEPT, socket, srvMgrTask.getTno()))
            {
                /* 发送失败的话, 需要在这里销毁套接字 */
                log.error("accept socket but send to server-mgr-task fail, and release socket");

                try
                {
                    socket.close();
                }
                catch (IOException e)
                {
                    log.error("close socket exception\n" + OssFunc.getExceptionInfo(e));
                }
            }
        }
    }
}
