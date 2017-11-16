package cn.zyy.oss.core.tcp;

import java.io.InputStream;
import java.net.Socket;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskPdc;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpRecv extends OssTaskPdc
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    private boolean             isOK;
    private Socket              socket;
    private IOssRecvHandler     recvHandler;

    protected OssTcpRecv(Socket recvSock, String taskName, int iPriority, IOssRecvHandler handler)
    {
        super(OssCoreConstants.TNO_INVALID, taskName, iPriority);

        socket = recvSock;

        /* 链路初始化是OK的 */
        isOK = true;

        recvHandler = handler;

        start();
    }

    public boolean isRecvOK()
    {
        return isOK;
    }

    private void closeSocketInput(InputStream recvStream)
    {
        if (null == recvStream)
        {
            return;
        }

        try
        {
            recvStream.close();
            log.info("socket input close success");
        }
        catch (Exception e)
        {
            log.error("socket input close exception\n" + OssFunc.getExceptionInfo(e));
        }
    }

    @Override
    public void onEntry()
    {
        InputStream recvStream = null;
        try
        {
            recvStream = socket.getInputStream();
        }
        catch (Exception e)
        {
            recvStream = null;
            log.error("get socket's input-stream exception\n%s" + OssFunc.getExceptionInfo(e));
        }

        if (null == recvStream)
        {
            isOK = false;

            log.info("task return");
            return;
        }

        while (true)
        {
            /* 判断下一步读操作的可用缓冲区大小：
             * --截止buff结尾
             * 或
             * --截至writePos */
            int recvBuffLen = recvHandler.buffSize4OnceRecv();
            int recvLen = 0;
            try
            {
                recvLen = recvStream.read(recvHandler.recvBuff, recvHandler.writePos, recvBuffLen);
            }
            catch (Exception e)
            {
                log.error("socket recv exception, and reset tcp-link\n" + OssFunc.getExceptionInfo(e));

                isOK = false;
                recvLen = 0;

                closeSocketInput(recvStream);
                return;
            }

            if (recvLen < 0)
            {
                log.info("recv data but return " + recvLen + ", and reset tcp-link");
                isOK = false;

                closeSocketInput(recvStream);
                return;
            }
            else if (0 == recvLen)
            {
                /* 如果为0, 则在onHandler中处理缓冲区满的情况 */
                log.info("recv data's len is 0, when write-pos=%s, recv-buff-len=%s", recvHandler.writePos, recvBuffLen);
            }

            /* 异常判断 */
            if (recvLen > recvBuffLen)
            {
                log.error("socket recv error, recvLen[" + recvLen + "] > recvBuffLen[" + recvBuffLen + "], and reset tcp-link");
                isOK = false;

                closeSocketInput(recvStream);
                return;
            }

            /* 移动缓冲区写Pos */
            recvHandler.writeByteNum(recvLen);
            log.debug("recv %s bytes msg, and recv-buff-status: %s", recvLen, recvHandler.toString());

            /* 组包, 将完整的数据包截取出来, 这里会有一下集中情况: 
             * 1) IOssRecvHandler.RET_LINK_ERROR: 表示链路异常错误, 需要断链重建链路
             * 2) IOssRecvHandler.RET_HANDLER_NOT_COMPLETE: 表示处理到一条不完整的数据, 需要继续从socket中read数据, 在处理
             * 3) IOssRecvHandler.RET_HANDLER_OK: 表示处理掉一条完整的数据
             * */
            while (true)
            {
                int retHandler = recvHandler.onHandler();
                if (IOssRecvHandler.RET_LINK_ERROR == retHandler)
                {
                    /* socket资源回收统一由管理任务实施 */
                    isOK = false;

                    log.error("recvHandler.onHandler error and set link not ok");

                    closeSocketInput(recvStream);
                    break;
                }

                if (IOssRecvHandler.RET_HANDLER_NOT_COMPLETE == retHandler)
                {
                    /* 不做任何处理, 继续接收socket数据 */
                    break;
                }

                if (IOssRecvHandler.RET_HANDLER_OK == retHandler)
                {
                    /* 如果接收缓冲区还有数据, 则继续从缓冲区中获取下一条数据 */
                    if (recvHandler.dataSize() <= 0)
                    {
                        log.debug("handler a complete-msg, and continue read socket. recv-buff-status: %s", recvLen, recvHandler.toString());

                        break;
                    }

                    log.debug("handler a complete-msg, and continue handler next-msg. recv-buff-status: %s", recvLen, recvHandler.toString());
                }
                else
                {
                    log.error("recvHandler.onHandler return a invalid ret[" + retHandler + "]");
                }
            }
        }
    }
}
