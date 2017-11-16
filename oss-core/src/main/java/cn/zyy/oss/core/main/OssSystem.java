package cn.zyy.oss.core.main;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import cn.zyy.oss.core.module.OssMgrModule;
import cn.zyy.oss.core.module.OssMonModule;
import cn.zyy.oss.core.module.OssTimerModule;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public abstract class OssSystem
{
    /*************************静态定义***************************/
    private static final OssLog log             = new OssLog(OssLog.LOG_MODULE_OSS);
    private static String       sysName         = "unknow";
    private static String       hostName        = "unknow";
    private static String       hostAddress     = "unknow";
    private static String       sysMonServerUrl = "http://10.1.1.1:11111/monitor";
    private static boolean      sysMonStatus    = false;

    public static String sysName()
    {
        return sysName;
    }

    public static String sysIp()
    {
        return hostAddress;
    }

    public static String sysHostName()
    {
        return hostName;
    }

    public static String monServerUrl()
    {
        return sysMonServerUrl;
    }

    public static boolean monStatus()
    {
        return sysMonStatus;
    }

    public static boolean sysInWorkStatus()
    {
        return (OssMgrModule.getInstance().getSysStatus() == OssCoreConstants.SYS_STATUS_WORK);
    }

    public static void setRunInfo(String runInfo)
    {
        String runInfoFile = System.getProperty(OssCoreConstants.ENV_RUN_INFO_FILE);
        if (null == runInfoFile)
        {
            runInfoFile = OssFunc.getPath(System.getProperty("user.dir"), OssCoreConstants.DEF_RUN_INFO_FILE);
        }

        if (null == runInfoFile)
        {
            log.error("null == program-run-dir");
            return;
        }
        OssFunc.writeToFile(runInfoFile, runInfo + "\n", false);
    }

    /*************************非静态定义***************************/
    /* 创建业务系统资源 */
    protected abstract void srvPeriodFuncPerMinutes();

    /* 创建业务系统资源 */
    protected abstract int createSrvResource(String[] args);

    /* 回收业务系统资源 */
    protected abstract int destroySrvResource();

    /* 设置系统名称 */
    protected void setSysName(String name)
    {
        sysName = name;
    }

    /* 设置监控服务Url */
    protected void setMonServerUrl(String url)
    {
        sysMonServerUrl = url;
    }

    /* 设置监控开关 */
    protected void setMonStatus(boolean open)
    {
        sysMonStatus = open;
    }

    private static int getSysStatus()
    {
        return OssMgrModule.getInstance().getSysStatus();
    }

    /* 创建支撑系统资源 */
    private int createOssResource()
    {
        /* 创建支撑系统任务管理模块 */
        OssMgrModule.getInstance();

        /* 创建支撑系统定时器模块 */
        OssTimerModule.getInstance();

        /* 创建支撑系统监控模块 */
        OssMonModule.getInstance();

        /* 获取系统ip、hostName */
        InetAddress netAddress = null;
        try
        {
            netAddress = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            netAddress = null;
        } ;

        if (null != netAddress)
        {
            hostName = netAddress.getHostName();
            hostAddress = netAddress.getHostAddress();
        }

        return OssCoreConstants.RET_OK;
    }

    /* 回收支撑系统资源 */
    private int destroyOssResource()
    {
        /* 关闭定时器模块 */

        /* 关闭系统监控客户端模块 */

        return OssCoreConstants.RET_OK;
    }

    public int registerSysMonitor(String type, int secNum, int sampleNum)
    {
        return OssMonModule.registerMonitor(type, secNum, sampleNum);
    }

    public boolean recordMonInfo(int id, long secTime, List<Long> lstSampleInfo)
    {
        if (!sysMonStatus)
        {
            return false;
        }

        return OssMonModule.recordMonInfo(id, secTime, lstSampleInfo);
    }

    @SuppressWarnings("restriction")
    private void setCloseSignal()
    {
        try
        {
            Signal sig = new Signal("TERM");
            Signal.handle(sig, new SignalHandler()
            {
                @Override
                public void handle(Signal arg0)
                {
                    /* 如果系统还没有进入工作状态, 则强制退出; 否则通过信号通知平缓退出 */
                    int iRet = OssMgrModule.getInstance().sendMsgEx(OssCoreConstants.MSG_ID_TASK_CLOSE, null, OssCoreConstants.TNO_FSM_OSS_MGR, OssCoreConstants.TNO_FSM_OSS_MGR);
                    if (OssConstants.RET_OK == iRet)
                    {
                        log.info("recv sys-close msg, and start close");
                    }
                    else
                    {
                        log.error("recv sys-close msg, but notify fail");
                    }

                    setRunInfo("recv quit notify");
                }
            });
        }
        catch (Exception e)
        {
            log.error("set signal TERM exception.\n" + OssFunc.getExceptionInfo(e));
        }
    }

    protected int runSystem(String[] args) throws Exception
    {
        /* 初始化日志配置 */
        if (!OssLog.initLogConfig())
        {
            log.error("oss-log initLogConfig error, and exit! please check the logback.xml");
            System.err.println("oss-log initLogConfig error, and exit! please check the logback.xml");
            setRunInfo("oss-log init error, and exit");
            return OssCoreConstants.RET_ERROR;
        }

        setRunInfo("start.....run system");
        if (OssCoreConstants.RET_OK != this.createOssResource())
        {
            log.info("create oss resource error, and exit");
            setRunInfo("create oss resource error, and exit");
            return OssCoreConstants.RET_ERROR;
        }
        else
        {
            log.info("create oss resource success......");
        }

        if (OssCoreConstants.RET_OK != this.createSrvResource(args))
        {
            log.info("create srv resource error, and exit");
            setRunInfo("create srv resource error, and exit");
            return OssCoreConstants.RET_ERROR;
        }
        else
        {
            log.info("create srv resource success......");
        }

        setRunInfo("create resource success, and start init");

        /* 系统资源创建完后, 开始启动工作 */
        log.info("system-name is " + sysName);
        log.info("\tsystem-ip is " + hostAddress);
        log.info("\tsystem-host-name is " + hostName);

        log.info("..............system start init...............");

        /* 启动定时器模块 */
        Thread.sleep(50);
        OssTimerModule.getInstance().sendMsgEx(OssCoreConstants.MSG_ID_TASK_INIT, null, OssCoreConstants.TNO_FSM_OSS_TIMER, OssCoreConstants.TNO_FSM_OSS_TIMER);

        /* 启动监控模块 */
        Thread.sleep(150);
        OssMonModule.getInstance().sendMsgEx(OssCoreConstants.MSG_ID_TASK_INIT, null, OssCoreConstants.TNO_FSM_OSS_MON, OssCoreConstants.TNO_FSM_OSS_MGR);

        /* 启动任务管理模块, 接着由任务管理模块启动应用层 */
        Thread.sleep(100);
        OssMgrModule.getInstance().sendMsgEx(OssCoreConstants.MSG_ID_TASK_INIT, null, OssCoreConstants.TNO_FSM_OSS_MGR, OssCoreConstants.TNO_FSM_OSS_MGR);

        /* 设置系统平缓退出信号handler */
        Thread.sleep(100);
        setCloseSignal();

        /* 维持系统主线程工作 */
        long lastPeriodSecond = System.currentTimeMillis() / 1000;
        while (true)
        {
            if (OssCoreConstants.SYS_STATUS_EXIT == getSysStatus())
            {
                /* 退出状态, 并且所有任务都已经退出, 则系统最终退出 */
                log.info("all-tasks close ok......");
                break;
            }

            long currentSecond = System.currentTimeMillis() / 1000;
            if (lastPeriodSecond + 60 <= currentSecond)
            {
                lastPeriodSecond = currentSecond;
                srvPeriodFuncPerMinutes();
            }

            Thread.sleep(1000);
        }

        /* 关闭业务系统资源 */
        if (OssCoreConstants.RET_OK != this.destroySrvResource())
        {
            log.info("destroy oss resource error, and exit");
            return OssCoreConstants.RET_ERROR;
        }
        else
        {
            log.info("destroy oss resource success......");
        }

        /* 启动支撑层系统资源 */
        if (OssCoreConstants.RET_OK != this.destroyOssResource())
        {
            log.info("destroy srv resource error, and exit");
            return OssCoreConstants.RET_ERROR;
        }
        else
        {
            log.info("destroy srv resource success......");
        }

        log.info("..............system quit ok...............");

        System.exit(0);
        return 0;
    }
}
