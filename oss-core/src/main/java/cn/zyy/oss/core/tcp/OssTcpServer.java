package cn.zyy.oss.core.tcp;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import javax.net.ServerSocketFactory;

import com.google.common.collect.Lists;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpServer extends OssTaskFsm
{
    private static final OssLog              log                      = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int                 MAX_TCP_SEND_BUFFER_SIZE = 2048 * 1024;
    private static final int                 MAX_TCP_RECV_BUFFER_SIZE = 2048 * 1024;
    private static final int                 MAX_TCP_CLIENT_LINK_NUM  = 1000;

    private static final int                 TIMER_BIND_CHECK         = OssCoreConstants.TIMER01;
    private static final int                 MSG_ID_TIMER_BIND_CHECK  = OssCoreConstants.MSG_ID_TIMER01;
    private static final int                 INTERIM_TIMER_BIND_CHECK = 30;

    private static final int                 TIMER_LINK_CHECK         = OssCoreConstants.TIMER02;
    private static final int                 MSG_ID_TIMER_LINK_CHECK  = OssCoreConstants.MSG_ID_TIMER02;
    private static final int                 INTERIM_TIMER_LINK_CHECK = 30;

    private OssTcpAccept                     acceptTask               = null;
    private List<OssTcpLink>                 lstTcpLink               = Lists.newArrayList();
    private int                              randomLinkIdx            = 0;
    private String                           serverIp;
    private int                              port;
    private int                              maxLinkNum               = 0;
    private Class<? extends IOssRecvHandler> handlerClass;

    protected OssTcpServer(int taskNo, String taskName, int iPriority, String serverIp, int port, int linkNum, Class<? extends IOssRecvHandler> handler)
    {
        super(taskNo, taskName, iPriority);

        this.serverIp = serverIp;
        this.port = port;
        this.maxLinkNum = linkNum;

        maxLinkNum = (maxLinkNum > MAX_TCP_CLIENT_LINK_NUM) ? MAX_TCP_CLIENT_LINK_NUM : maxLinkNum;
        for (int idxLink = 0; idxLink < maxLinkNum; idxLink++)
        {
            OssTcpLink tmpLink = new OssTcpLink();
            tmpLink.isUse = false;
            lstTcpLink.add(tmpLink);
        }

        this.handlerClass = handler;
    }

    protected void closeSocket(ServerSocket socket)
    {
        if (null != socket)
        {
            try
            {
                socket.close();
            }
            catch (Exception e)
            {
                log.error("close server-socket exception\n" + OssFunc.getExceptionInfo(e));
            }
        }
    }

    protected void closeSocket(Socket socket)
    {
        if (null != socket)
        {
            try
            {
                socket.close();
            }
            catch (Exception e)
            {
                log.error("close socket exception\n" + OssFunc.getExceptionInfo(e));
            }
        }
    }

    private ServerSocket createBind()
    {
        ServerSocket socket = null;
        try
        {
            socket = ServerSocketFactory.getDefault().createServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(serverIp, port), 20);
        }
        catch (Exception e)
        {
            String exceptionInfo = (null == e) ? "null" : e.toString();
            log.error("create server-socket[%s-%s] exception: \n%s", serverIp, port, exceptionInfo);

            if (null != socket)
            {
                try
                {
                    socket.close();
                }
                catch (Exception e1)
                {
                    log.error("create server-socket fail, and close it exception\n" + OssFunc.getExceptionInfo(e1));
                }
            }

            return null;
        }

        return socket;
    }

    private void checkBind()
    {
        /* 如果有监听任务, 则判断任务状态 */
        if (null != acceptTask)
        {
            if (!acceptTask.isAlive())
            {
                /* 监听任务挂了 */
                acceptTask.closeSocket();
                acceptTask = null;

                log.error("accept-task is not alive, and release task");
            }
            else if (!acceptTask.isAcceptOK())
            {
                /* 套接字状态错误 */
                acceptTask.closeSocket();
                acceptTask = null;

                log.error("accept-task not accept ok, and release task");
            }
            else
            {
                /* 监听状态OK, 则检查OK */
            }
        }

        if (null == acceptTask)
        {
            /* 如果没有监听任务, 则创建任务 */
            ServerSocket bindSocket = createBind();
            if (null == bindSocket)
            {
                return;
            }

            /* 创建监听任务 */
            OssTcpAccept tmpAcceptTask = new OssTcpAccept(this, bindSocket, getTaskName() + "-accept", getPriority());
            if (!tmpAcceptTask.isTaskValid())
            {
                log.error("create accept-task for server-socket[%s-%s] fail", serverIp, port);

                closeSocket(bindSocket);
                return;
            }

            acceptTask = tmpAcceptTask;

            log.error("create accept-task for server-socket[%s-%s] success", serverIp, port);
        }
    }

    protected int createTcpLink(Socket socket)
    {
        int canUseLinkIdx = 0;
        for (canUseLinkIdx = 0; canUseLinkIdx < maxLinkNum; canUseLinkIdx++)
        {
            if (!lstTcpLink.get(canUseLinkIdx).isUse)
            {
                break;
            }
        }

        String clientIp = socket.getInetAddress().toString();
        int clientPort = socket.getPort();

        if (canUseLinkIdx >= maxLinkNum)
        {
            log.error("create-tcp-link client[%s-%s] fail, no can-use link exist", clientIp, clientPort);
            return OssCoreConstants.RET_ERROR;
        }

        /* 为该tcp链接创建发送任务, 并且判断任务是否创建成功 */
        OssTcpSend tcpSendTask = new OssTcpSend(socket, getTaskName() + "-link" + canUseLinkIdx, getPriority());
        if (!tcpSendTask.isTaskValid())
        {
            log.error("create-tcp-link[%s] to client[%s-%s] fail. connect success, but create send-task invalid", canUseLinkIdx, clientIp, clientPort);
            return OssCoreConstants.RET_ERROR;
        }

        /* 为该tcp链接创建接收任务 */
        IOssRecvHandler tcpRecvHandler = null;
        try
        {
            tcpRecvHandler = (IOssRecvHandler) handlerClass.newInstance();
            tcpRecvHandler.setRoleTrueClt_FalseSrv(false);
            tcpRecvHandler.setLinkIdx(canUseLinkIdx);
        }
        catch (Exception e)
        {
            tcpRecvHandler = null;
            log.error("new tcp recv-handler class exception\n" + OssFunc.getExceptionInfo(e));

            sendMsg(OssCoreConstants.MSG_ID_TASK_KILL, null, tcpSendTask.getTno());
            return OssCoreConstants.RET_ERROR;
        }

        OssTcpRecv tcpRecvTask = new OssTcpRecv(socket, getTaskName() + "-link" + canUseLinkIdx, getPriority(), tcpRecvHandler);
        if (!tcpRecvTask.isTaskValid())
        {
            log.error("create-tcp-link[%s] to server[%s-%s] fail. connect success, but create recv-task invalid", canUseLinkIdx, serverIp, port);

            sendMsg(OssCoreConstants.MSG_ID_TASK_KILL, null, tcpSendTask.getTno());
            return OssCoreConstants.RET_ERROR;
        }

        OssTcpLink tcpLink = lstTcpLink.get(canUseLinkIdx);
        tcpLink.socket = socket;
        tcpLink.sendTask = tcpSendTask;
        tcpLink.recvTask = tcpRecvTask;
        tcpLink.isClt = false;
        tcpLink.isUse = true;

        log.info("create-tcp-link[%s] success: ", tcpLink.toString());

        return OssCoreConstants.RET_OK;
    }

    protected void closeTcpLink(int idxLink)
    {
        if (idxLink >= maxLinkNum)
        {
            log.error("close-tcp-link[" + idxLink + "] invalid, cannot >= " + maxLinkNum);
            return;
        }
        OssTcpLink tcpLink = lstTcpLink.get(idxLink);

        /* 先关闭链路 */
        tcpLink.isUse = false;

        /* 先关闭socket; 如果发送任务还有消息发送, 则会探测到socket失败, 自动销毁任务 */
        closeSocket(tcpLink.socket);
        tcpLink.socket = null;

        /* 关闭"发送任务"发送关闭通知, 如果没有消息发送, 该通知会使得发送任务自动销毁 */
        if (null != tcpLink.sendTask)
        {
            sendMsg(OssCoreConstants.MSG_ID_TASK_KILL, null, tcpLink.sendTask.getTno());
            tcpLink.sendTask = null;
        }

        /* "接收任务"会自动探测链路不可用, 自动销毁 */
        if (null != tcpLink.recvTask)
        {
            tcpLink.recvTask = null;
        }
    }

    private void checkLink(int idxLink)
    {
        if (idxLink >= maxLinkNum)
        {
            log.error("check-tcp-link[" + idxLink + "] invalid, cannot >= " + maxLinkNum);
            return;
        }
        OssTcpLink tcpLink = lstTcpLink.get(idxLink);

        /* 如果链路是关闭状态, 则回收 */
        if (!tcpLink.isUse)
        {
            closeTcpLink(idxLink);
            return;
        }

        /* 如果发送异常, 则关闭链路 */
        if (null == tcpLink.sendTask)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s sendTask=null, when in use-status");
        }

        if (null == tcpLink.sendTask || (!tcpLink.sendTask.isSendOK()))
        {
            log.error("check-tcp-link error, link[" + idxLink + "] send-status not OK when in use-status, and release link");

            closeTcpLink(idxLink);
            return;
        }

        /* 如果接收异常, 则关闭链路 */
        if (null == tcpLink.recvTask)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s recvTask=null, when in use-status");
        }

        if (null == tcpLink.recvTask || (!tcpLink.recvTask.isRecvOK()))
        {
            log.error("check-tcp-link error, link[" + idxLink + "] recv-status not OK when in use-status, and release link");

            closeTcpLink(idxLink);
            return;
        }

        /* 判断链路是否异常: 输入 */
        if (null == tcpLink.socket)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s socket=null, close and release link");

            closeTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isClosed())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s socket close, close and release link");

            closeTcpLink(idxLink);
            return;
        }

        if (!tcpLink.socket.isConnected())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s connect break, close and release link");

            closeTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isOutputShutdown())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s output-half break, close and release link");

            closeTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isInputShutdown())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s input-half break, close and release link");

            closeTcpLink(idxLink);
            return;
        }
    }

    @Override
    protected int init()
    {
        setTimer(TIMER_BIND_CHECK, 2);

        setTimer(TIMER_LINK_CHECK, 20);
        return OssCoreConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        switch (msgId)
        {
            case MSG_ID_TIMER_BIND_CHECK:
            {
                for (int idxLink = 0; idxLink < maxLinkNum; idxLink++)
                {
                    try
                    {
                        checkBind();
                    }
                    catch (Exception e)
                    {
                        log.error("check link[" + idxLink + "] exception\n" + OssFunc.getExceptionInfo(e));
                    }
                }

                setTimer(TIMER_BIND_CHECK, INTERIM_TIMER_BIND_CHECK);
                break;
            }

            case MSG_ID_TIMER_LINK_CHECK:
            {
                for (int idxLink = 0; idxLink < maxLinkNum; idxLink++)
                {
                    try
                    {
                        checkLink(idxLink);
                    }
                    catch (Exception e)
                    {
                        log.error("check link[" + idxLink + "] exception\n" + OssFunc.getExceptionInfo(e));
                    }
                }

                setTimer(TIMER_LINK_CHECK, INTERIM_TIMER_LINK_CHECK);
                break;
            }

            case OssCoreConstants.MSG_ID_TCP_ACCEPT:
            {
                if (null == objContext || !(objContext instanceof Socket))
                {
                    log.error("recv MSG_ID_TCP_ACCEPT msg, but objContext is invalid");
                    break;
                }
                Socket socket = (Socket) objContext;

                /* 新建链路 */
                if (OssCoreConstants.RET_OK != createTcpLink(socket))
                {
                    closeSocket(socket);
                }
                break;
            }
        }
    }

    @Override
    protected int close()
    {
        for (int idxLink = 0; idxLink < maxLinkNum; idxLink++)
        {
            try
            {
                closeTcpLink(idxLink);
            }
            catch (Exception e)
            {
                log.error("check link[" + idxLink + "] exception\n" + OssFunc.getExceptionInfo(e));
            }
        }

        return OssCoreConstants.RET_OK;
    }

    public int send(int srcTno, int commLinkIdx, byte[] msg)
    {
        /* 构造消息头部 */
        byte[] headBytes = IOssRecvHandler.getSrvMsgHead(msg.length);

        /* 组装消息TCP消息体 */
        byte[] sendBytes = new byte[headBytes.length + msg.length];
        System.arraycopy(headBytes, 0, sendBytes, 0, headBytes.length);
        System.arraycopy(msg, 0, sendBytes, headBytes.length, msg.length);

        OssTcpLink tcpLink = lstTcpLink.get(commLinkIdx);
        if (!tcpLink.isUse || (null == tcpLink.sendTask) || (!tcpLink.sendTask.isSendOK()))
        {
            log.error("link[%s] invalid, rsp fail.");
            return OssConstants.RET_ERROR;
        }

        log.debug("link[%s] rsp msg-len=%s", commLinkIdx, sendBytes.length);
        return tcpLink.sendTask.sendData(srcTno, sendBytes);
    }
}
