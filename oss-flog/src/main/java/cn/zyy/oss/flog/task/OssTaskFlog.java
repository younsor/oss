package cn.zyy.oss.flog.task;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import javax.net.SocketFactory;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.flog.OssFlog;
import cn.zyy.oss.flog.file.OssShareCache;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTaskFlog extends OssTaskFsm
{
    private static final OssLog log                            = new OssLog(OssLog.LOG_MODULE_OSS);

    private static final int    TIMER_GET_DATA                 = OssCoreConstants.TIMER01;
    private static final int    MSG_ID_TIMER_GET_DATA          = OssCoreConstants.MSG_ID_TIMER01;
    private static final int    INTERIM_CHECK_GET_DATA         = 1;

    private static final int    TIMER_CHECK_CONNECT            = OssCoreConstants.TIMER02;
    private static final int    MSG_ID_TIMER_CHECK_CONNECT     = OssCoreConstants.MSG_ID_TIMER02;
    private static final int    INTERIM_CHECK_CONNECT          = 30;

    private static final int    INTERIM_PRINT_TASK_STATUS_INFO = 300000;                           /* 每5分钟打印一次 */

    private OssFlog             parentModule                   = null;
    private Socket              cltSocket                      = null;
    private OssShareCache       cacheFile                      = null;

    private long                lastPrintInfoMillTime;
    private long                statSendSuccessNum             = 0;
    private long                statSendFailNum                = 0;
    private long                flumeLinkBreakNum              = 0;

    public OssTaskFlog(int taskNo, String taskName, int iPriority, OssFlog module)
    {
        super(taskNo, taskName, iPriority);

        parentModule = module;
    }

    private Socket createsSocketAndConnect()
    {
        String flumeIp = parentModule.getFlumeIp();
        int flumePort = parentModule.getFlumePort();
        int oldRecvBuffSize = 0;
        int oldSendBuffSize = 0;
        int recvBuffSize = 0;
        int sendBuffSize = 0;
        Socket socket = null;
        try
        {
            socket = SocketFactory.getDefault().createSocket(flumeIp, flumePort);

            /* 禁用小包等待, flume日志通常都是大包 */
            socket.setTcpNoDelay(false);

            /* 开启无数据交互时的链路心跳功能 */
            socket.setKeepAlive(true);

            oldSendBuffSize = socket.getSendBufferSize();
            oldRecvBuffSize = socket.getReceiveBufferSize();

            socket.setSendBufferSize(4096 * 1024);
            socket.setReceiveBufferSize(4096 * 1024);

            sendBuffSize = socket.getSendBufferSize();
            recvBuffSize = socket.getReceiveBufferSize();
        }
        catch (IOException e)
        {
            log.error("createSocket to flume server[%s-%s] error \n%s", flumeIp, flumePort, OssFunc.getExceptionInfo(e));

            return null;
        }

        cltSocket = socket;

        log.info("createSocket to flume server[%s-%s] success, and local=%s-%s, send-buf[%s->%s], recv-buff[%s->%s]", parentModule.getFlumeIp(), parentModule.getFlumePort(), cltSocket.getLocalAddress(), cltSocket.getLocalPort(), oldSendBuffSize,
                sendBuffSize, oldRecvBuffSize, recvBuffSize);
        return socket;
    }

    protected void closeSocket(Socket socket)
    {
        if (socket == null)
        {
            return;
        }

        try
        {
            socket.close();
        }
        catch (IOException ioe)
        {
            if (!"Socket is closed".equalsIgnoreCase(ioe.getMessage()))
            {
                throw new RuntimeException(ioe);
            }
        }
        finally
        {
            if (socket == this.cltSocket)
            {
                this.cltSocket = null;
            }
        }
    }

    @Override
    protected int init()
    {
        cltSocket = createsSocketAndConnect();
        if (null == cltSocket)
        {
            return OssConstants.RET_ERROR;
        }

        /* 初始化Flume缓存文件 */
        int taskIdx = getTno() - OssCoreConstants.TNO_FSM_CORE_FLOG_START;
        String cacheModuleName = String.format("%s%02d", parentModule.getLogIdent(), taskIdx);
        cacheFile = new OssShareCache(cacheModuleName, parentModule.getCacheDir(), 30000);
        if (OssConstants.RET_OK != cacheFile.init())
        {
            return OssConstants.RET_ERROR;
        }

        /* 日志消息处理定时器 */
        setTimer(TIMER_GET_DATA, INTERIM_CHECK_GET_DATA);

        lastPrintInfoMillTime = System.currentTimeMillis();
        return OssConstants.RET_OK;
    }

    public int addData(byte[] byteData)
    {
        int iRet = cacheFile.add(byteData);

        // log.debug(statusInfo());

        return iRet;
    }

    public long getSendSuccessNum()
    {
        return statSendSuccessNum;
    }

    public long getSendFailNum()
    {
        return statSendFailNum;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        checkAndPrintStatusInfo();

        switch (msgId)
        {
            case MSG_ID_TIMER_GET_DATA:
            {
                Object objData = null;
                while (null != (objData = cacheFile.get()))
                {
                    // log.debug(statusInfo());

                    if (!(objData instanceof byte[]))
                    {
                        log.error("get a buff-data, buf not instanceof byte[]");
                        break;
                    }

                    byte[] byteData = (byte[]) objData;
                    try
                    {
                        // cltSocket.setSendBufferSize(byteData.length);
                        OutputStream tcpOutput = cltSocket.getOutputStream();
                        tcpOutput.write(byteData);
                    }
                    catch (Exception e)
                    {
                        statSendFailNum++;

                        /* 发送异常的话, 把数据重新放入缓存区 */
                        cacheFile.add(byteData);

                        flumeLinkBreakNum++;
                        log.error("cltSocket send exception...%s", OssFunc.getExceptionInfo(e));
                        closeSocket(cltSocket);

                        /* 退出发送流程, 开始重建链路 */
                        setTimer(TIMER_CHECK_CONNECT, INTERIM_CHECK_CONNECT);
                        return;
                    }

                    statSendSuccessNum++;

                    // log.debug("send log msg[len=%s] ... OK", byteData.length);
                }

                /* 没有数据了, 定期检查数据 */
                setTimer(TIMER_GET_DATA, INTERIM_CHECK_GET_DATA);
                break;
            }

            case MSG_ID_TIMER_CHECK_CONNECT:
            {
                if (null != cltSocket && (!cltSocket.isClosed()) && cltSocket.isConnected())
                {
                    /* 此时链路不应该是OK的 */
                    log.error("cltSocket's connect status error, should not OK.");

                    setTimer(TIMER_GET_DATA, INTERIM_CHECK_GET_DATA);
                    return;
                }

                if (null != cltSocket && (!cltSocket.isClosed()))
                {
                    try
                    {
                        cltSocket.close();
                        log.info("close socket before create-and-connect again");
                    }
                    catch (IOException e)
                    {
                        log.error("cltSocket.close error. \n%s", OssFunc.getExceptionInfo(e));
                    }
                }

                cltSocket = createsSocketAndConnect();
                if (null == cltSocket)
                {
                    /* 里面输出了日志, 这里不在输出 */
                    setTimer(TIMER_CHECK_CONNECT, INTERIM_CHECK_CONNECT);
                }
                else
                {
                    /* 建链成功, 启动日志发送 */
                    log.info("createSocket to flume server[%s-%s] success, and local=%s-%s", parentModule.getFlumeIp(), parentModule.getFlumePort(), cltSocket.getLocalAddress().toString(), cltSocket.getLocalPort());

                    setTimer(TIMER_GET_DATA, INTERIM_CHECK_GET_DATA);
                }

                break;
            }

            default:
            {
                log.error("work: recv unknown msg, msg-id=" + msgId + " from tno[" + getSender() + "]");
                break;
            }
        }
    }

    @Override
    protected int close()
    {
        return 0;
    }

    private void checkAndPrintStatusInfo()
    {
        long curMillTime = System.currentTimeMillis();
        if (curMillTime - lastPrintInfoMillTime >= INTERIM_PRINT_TASK_STATUS_INFO)
        {
            log.info(statusInfo());
            lastPrintInfoMillTime = curMillTime;
        }
    }

    public String statusInfo()
    {
        StringBuilder sbInfo = new StringBuilder();
        sbInfo.append(getTaskName() + "'s status info is as follow: \n");
        sbInfo.append("\ttotal-send-success-num=" + statSendSuccessNum);
        sbInfo.append("\ttotal-send-fail-num=" + statSendFailNum);
        sbInfo.append("\tflume-tcp-link-break-num=" + flumeLinkBreakNum + "\n");
        sbInfo.append(cacheFile.toString());

        return sbInfo.toString();
    }
}
