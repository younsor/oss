package cn.zyy.oss.flog;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.flog.file.OssShareCache;
import cn.zyy.oss.flog.task.OssTaskFlog;
import cn.zyy.oss.share.OssLog;

public class OssFlog
{
    private static final OssLog log       = new OssLog(OssLog.LOG_MODULE_OSS);
    private String              hostName  = "localhost";
    private String              flumeIp   = null;
    private int                 flumePort = 0;
    private String              logIdent  = null;
    private String              cacheDir  = null;

    public void setFlumeIp(String ip)
    {
        flumeIp = ip;

        log.info("set ossflog-module's ip: " + flumeIp);
    }

    public void setFlumePort(int port)
    {
        flumePort = port;

        log.info("set ossflog-module's port: " + port);
    }

    public void setCacheDir(String dir)
    {
        cacheDir = dir;

        log.info("set ossflog-cache-dir: " + cacheDir);
    }

    public String getFlumeIp()
    {
        return flumeIp;
    }

    public int getFlumePort()
    {
        return flumePort;
    }

    public String getCacheDir()
    {
        return cacheDir;
    }

    public String getLogIdent()
    {
        return logIdent;
    }

    public String getHostName()
    {
        return hostName;
    }

    private AtomicLong        logNum;
    private int               taskNum;
    private List<OssTaskFlog> lstFlogTask = new ArrayList<OssTaskFlog>();

    public OssFlog(String name, int num)
    {
        if (null == name)
        {
            logIdent = "null";
        }
        else
        {
            logIdent = name;
        }

        /* 获取HostName */
        try
        {
            hostName = (InetAddress.getLocalHost()).getHostName();
        }
        catch (UnknownHostException uhe)
        {
            String host = uhe.getMessage(); // host = "hostname: hostname"
            if (host != null)
            {
                int colon = host.indexOf(':');
                if (colon > 0)
                {
                    hostName = host.substring(0, colon);
                }
            }

            hostName = "localhost";
        }

        /* TODO 判断日志缓存目录中有多少个缓存文件, 如果缓存目录数量与线程数量 */
        logNum = new AtomicLong(0);
        this.taskNum = num;
        for (int idx = 0; idx < taskNum; idx++)
        {
            OssTaskFlog flogTask = new OssTaskFlog(OssCoreConstants.TNO_FSM_CORE_START + idx, "flog" + idx, 0, this);
            lstFlogTask.add(flogTask);
        }

        log.info("init ossflog-module taskNum=" + taskNum + ", ident=" + logIdent + ", hostName=" + hostName);
    }

    public int log(byte[] byteLog, int srcTno)
    {
        long logSeqNo = logNum.addAndGet(1);
        int taskIdx = (int) (logSeqNo % taskNum);
        if (taskIdx < 0)
        {
            taskIdx = 0 - taskIdx;
        }

        int destTno = OssCoreConstants.TNO_FSM_CORE_START + taskIdx;
        int iRet = lstFlogTask.get(taskIdx).addData(byteLog);
        if (OssShareCache.RET_OK != iRet)
        {
            log.error("flume-log add to flog-task(%s) fail, when logSeqNo=%s, srcTno=%s, log-length=%s, iRet=%s", destTno, logSeqNo, srcTno, byteLog.length, iRet);
        }
        else
        {
            log.debug("flume-log add to flog-task(%s) success, when logSeqNo=%s, srcTno=%s, log-length=%s", destTno, logSeqNo, srcTno, byteLog.length);
        }

        return iRet;
    }

    public long getSendSuccessNum()
    {
        long successNum = 0;
        for (int idx = 0; idx < lstFlogTask.size(); idx++)
        {
            successNum += lstFlogTask.get(idx).getSendSuccessNum();
        }

        return successNum;
    }

    public long getSendFailNum()
    {
        long failNum = 0;
        for (int idx = 0; idx < lstFlogTask.size(); idx++)
        {
            failNum += lstFlogTask.get(idx).getSendFailNum();
        }

        return failNum;
    }
}
