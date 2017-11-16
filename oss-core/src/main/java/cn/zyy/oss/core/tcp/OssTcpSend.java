package cn.zyy.oss.core.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskCsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTcpSend extends OssTaskCsm
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    private boolean             isOK;
    private Socket              socket;
    private OutputStream        sockOutput;

    protected OssTcpSend(Socket sendSock, String taskName, int iPriority)
    {
        super(OssCoreConstants.TNO_INVALID, taskName, iPriority);

        socket = sendSock;

        /* 链路初始化是OK的 */
        isOK = true;

        try
        {
            sockOutput = socket.getOutputStream();
        }
        catch (IOException e)
        {
            log.error("get socket's output-stream exception\n" + OssFunc.getExceptionInfo(e));

            sockOutput = null;
            isOK = false;
        }
    }

    public boolean isSendOK()
    {
        return isOK;
    }

    public int sendData(int srcTno, byte[] msg)
    {
        return sendMsgEx(OssCoreConstants.MSG_ID_TCP_SEND, msg, getTno(), srcTno);
    }

    @Override
    public int onHandler(int msgId, Object objContext)
    {
        if (!isOK)
        {
            log.error("handler to send socket-msg error, because send-link is not OK");
            return OssConstants.RET_ERROR;
        }

        if (!(objContext instanceof byte[]))
        {
            log.error("handler to send socket-msg error, because send-msg is not instanceof byte[]");
            return OssConstants.RET_OK;
        }
        byte[] sendMsg = (byte[]) objContext;

        try
        {
            sockOutput.write(sendMsg);
        }
        catch (IOException e)
        {
            log.error("socket send msg[len=" + sendMsg.length + "] exception\n" + OssFunc.getExceptionInfo(e));

            /* 将链路置异常 */
            isOK = false;

            closeSocketOutput(sockOutput);
            sockOutput = null;

            return OssConstants.RET_ERROR;
        }

        return OssCoreConstants.RET_OK;
    }

    private void closeSocketOutput(OutputStream outputStream)
    {
        if (null == outputStream)
        {
            return;
        }

        try
        {
            outputStream.close();
            log.info("socket output close success");
        }
        catch (Exception e)
        {
            log.error("socket output close exception\n" + OssFunc.getExceptionInfo(e));
        }
    }

    @Override
    protected void quit()
    {
        closeSocketOutput(sockOutput);
        sockOutput = null;

        return;
    }
}
