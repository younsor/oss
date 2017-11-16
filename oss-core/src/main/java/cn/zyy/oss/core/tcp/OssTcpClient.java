package cn.zyy.oss.core.tcp;

import java.net.Socket;
import java.util.List;

import javax.net.SocketFactory;

import com.google.common.collect.Lists;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpClient extends OssTaskFsm
{
    private static final OssLog              log                           = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int                 MAX_TCP_SEND_BUFFER_SIZE      = 2048 * 1024;
    private static final int                 MAX_TCP_RECV_BUFFER_SIZE      = 2048 * 1024;
    private static final int                 MAX_TCP_CLIENT_LINK_NUM       = 300;

    private static final int                 TIMER_LINK_CHECK              = OssCoreConstants.TIMER01;
    private static final int                 MSG_ID_TIMER_TIMER_LINK_CHECK = OssCoreConstants.MSG_ID_TIMER01;
    private static final int                 INTERIM_TIMER_LINK_CHECK      = 30;

    private List<OssTcpLink>                 lstTcpLink                    = Lists.newArrayList();
    private int                              randomLinkIdx                 = 0;
    private String                           serverIp;
    private int                              port;
    private int                              maxLinkNum                    = 0;
    private Class<? extends IOssRecvHandler> handlerClass;

    protected OssTcpClient(int taskNo, String taskName, int iPriority, String serverIp, int port, int cltLinkNum, Class<? extends IOssRecvHandler> handler)
    {
        super(taskNo, taskName, iPriority);

        this.serverIp = serverIp;
        this.port = port;
        this.maxLinkNum = cltLinkNum;

        maxLinkNum = (maxLinkNum > MAX_TCP_CLIENT_LINK_NUM) ? MAX_TCP_CLIENT_LINK_NUM : maxLinkNum;
        for (int idxLink = 0; idxLink < maxLinkNum; idxLink++)
        {
            OssTcpLink tmpLink = new OssTcpLink();
            tmpLink.isUse = false;
            tmpLink.isClt = true;

            lstTcpLink.add(tmpLink);
        }

        this.handlerClass = handler;
    }

    private boolean createTcpLink(int idxLink)
    {
        if (idxLink >= maxLinkNum)
        {
            log.error("create-tcp-link: link(" + idxLink + ") invalid, cannot >= " + maxLinkNum);
            return false;
        }

        int recvBuffSize = 0;
        int sendBuffSize = 0;
        Socket socket = null;
        try
        {
            socket = SocketFactory.getDefault().createSocket(serverIp, port);

            /* 禁用小包等待, flume日志通常都是大包 */
            socket.setTcpNoDelay(false);

            /* 开启无数据交互时的链路心跳功能 */
            socket.setKeepAlive(true);

            socket.setSendBufferSize(MAX_TCP_SEND_BUFFER_SIZE);
            socket.setReceiveBufferSize(MAX_TCP_RECV_BUFFER_SIZE);

            sendBuffSize = socket.getSendBufferSize();
            recvBuffSize = socket.getReceiveBufferSize();
        }
        catch (Exception e)
        {
            String exceptionInfo = (null == e) ? "null" : e.toString();
            log.error("create-tcp-link[%s] to server[%s-%s] exception: \n%s", idxLink, serverIp, port, exceptionInfo);

            if (null != socket)
            {
                try
                {
                    socket.close();
                }
                catch (Exception e1)
                {
                    log.error("create-tcp-link[" + idxLink + "] fail, and close it exception\n" + OssFunc.getExceptionInfo(e1));
                }
            }

            return false;
        }

        /* 为该tcp链接创建发送任务, 并且判断任务是否创建成功 */
        OssTcpSend tcpSendTask = new OssTcpSend(socket, getTaskName() + "-link" + idxLink, getPriority());
        if (!tcpSendTask.isTaskValid())
        {
            log.error("create-tcp-link[%s] to server[%s-%s] fail. connect success, but create send-task invalid", idxLink, serverIp, port);
            closeSocket(socket);
            return false;
        }

        /* 为该tcp链接创建接收任务 */
        IOssRecvHandler tcpRecvHandler = null;
        try
        {
            tcpRecvHandler = (IOssRecvHandler) handlerClass.newInstance();
            tcpRecvHandler.setRoleTrueClt_FalseSrv(true);
            tcpRecvHandler.setLinkIdx(idxLink);
        }
        catch (Exception e)
        {
            tcpRecvHandler = null;
            log.error("new tcp recv-handler class exception\n" + OssFunc.getExceptionInfo(e));

            closeSocket(socket);

            sendMsg(OssCoreConstants.MSG_ID_TASK_KILL, null, tcpSendTask.getTno());
            return false;
        }

        OssTcpRecv tcpRecvTask = new OssTcpRecv(socket, getTaskName() + "-link" + idxLink, getPriority(), tcpRecvHandler);
        if (!tcpRecvTask.isTaskValid())
        {
            log.error("create-tcp-link[%s] to server[%s-%s] fail. connect success, but create recv-task invalid", idxLink, serverIp, port);
            closeSocket(socket);

            sendMsg(OssCoreConstants.MSG_ID_TASK_KILL, null, tcpSendTask.getTno());
            return false;
        }

        OssTcpLink tcpLink = lstTcpLink.get(idxLink);
        tcpLink.socket = socket;
        tcpLink.sendTask = tcpSendTask;
        tcpLink.recvTask = tcpRecvTask;
        tcpLink.isClt = true;
        tcpLink.isUse = true;

        log.info("create-tcp-link[%s] local[%s-%s] to server[%s-%s] success, sock-buf[%s-%s], send-tno=%s, recv-tno=%s", idxLink, socket.getLocalAddress(), socket.getLocalPort(), serverIp, port, sendBuffSize, recvBuffSize, tcpLink.sendTask.getTno(),
                tcpLink.recvTask.getTno());

        return true;
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

        /* 如果链路是关闭状态, 则创建 */
        if (!tcpLink.isUse)
        {
            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        /* 如果发送异常, 则关闭链路, 再重新建链 */
        if (null == tcpLink.sendTask)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s sendTask=null, when in use-status");
        }

        if (null == tcpLink.sendTask || (!tcpLink.sendTask.isSendOK()))
        {
            log.error("check-tcp-link error, link[" + idxLink + "] send-status not OK when in use-status, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        /* 如果接收异常, 则关闭链路, 再重新建链 */
        if (null == tcpLink.recvTask)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s recvTask=null, when in use-status");
        }

        if (null == tcpLink.recvTask || (!tcpLink.recvTask.isRecvOK()))
        {
            log.error("check-tcp-link error, link[" + idxLink + "] recv-status not OK when in use-status, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        /* 判断链路是否异常: 输入 */
        if (null == tcpLink.socket)
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s socket=null, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isClosed())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s socket close, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        if (!tcpLink.socket.isConnected())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s connect break, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isOutputShutdown())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s output-half break, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }

        if (tcpLink.socket.isInputShutdown())
        {
            log.error("check-tcp-link error, link[" + idxLink + "]'s input-half break, close and re-create link...");

            closeTcpLink(idxLink);
            createTcpLink(idxLink);
            return;
        }
    }

    @Override
    protected int init()
    {
        setTimer(TIMER_LINK_CHECK, 2);
        return OssCoreConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        switch (msgId)
        {
            case MSG_ID_TIMER_TIMER_LINK_CHECK:
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

    public int send(int srcTno, byte[] msg)
    {
        /* 构造消息头部 */
        byte[] headBytes = IOssRecvHandler.getCltMsgHead(msg.length);

        /* 组装消息TCP消息体 */
        byte[] sendBytes = new byte[headBytes.length + msg.length];
        System.arraycopy(headBytes, 0, sendBytes, 0, headBytes.length);
        System.arraycopy(msg, 0, sendBytes, headBytes.length, msg.length);

        int curSendLinkIdx = (randomLinkIdx++) % maxLinkNum;

        for (int sendLinkNum = 0; sendLinkNum < maxLinkNum; sendLinkNum++)
        {
            OssTcpLink tcpLink = lstTcpLink.get(curSendLinkIdx);
            if (!tcpLink.isUse || (null == tcpLink.sendTask) || (!tcpLink.sendTask.isSendOK()))
            {
                curSendLinkIdx = ((curSendLinkIdx + 1) % maxLinkNum);
                continue;
            }

            log.debug("send msg-len=%s", sendBytes.length);
            return tcpLink.sendTask.sendData(srcTno, sendBytes);
        }

        return OssConstants.RET_ERROR;
    }
}
