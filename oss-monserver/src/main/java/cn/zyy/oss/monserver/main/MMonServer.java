package cn.zyy.oss.monserver.main;

import java.io.File;

import cn.zyy.oss.core.main.OssSystem;
import cn.zyy.oss.monserver.conf.MConfModule;
import cn.zyy.oss.monserver.conf.MSysConfig;
import cn.zyy.oss.monserver.share.MConstants;
import cn.zyy.oss.monserver.srv.MSrvModule;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class MMonServer extends OssSystem
{
    private static final OssLog log     = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final OssLog perfLog = new OssLog();

    /***************************************全局定义*****************************************/
    private static MMonServer   system  = null;

    public static MMonServer getInstance()
    {
        if (null == system)
        {
            system = new MMonServer();
        }

        return system;
    }

    public static MSrvModule getSrvModule()
    {
        return getInstance().srvModule;
    }

    public static MConfModule getConfModule()
    {
        return getInstance().confModule;
    }

    public static MSysConfig getSysConfig()
    {
        return getConfModule().getSysConfig();
    }

    public static OssLog perfLog()
    {
        return perfLog;
    }

    /***************************************局部定义*****************************************/
    private MMonServer()
    {}

    /* 系统资源-业务模块 */
    private MSrvModule  srvModule  = null;

    /* 系统资源-配置模块 */
    private MConfModule confModule = null;

    @Override
    public int createSrvResource(String[] args)
    {
        String confFile = args[0];

        /* 系统资源-配置模块 */
        try
        {
            confModule = new MConfModule(confFile, OssConstants.TASK_PRIORITY_0);
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return MConstants.RET_ERROR;
        }

        /* 系统资源-业务模块 */
        srvModule = new MSrvModule(OssConstants.TASK_PRIORITY_2);

        return MConstants.RET_OK;
    }

    @Override
    public int destroySrvResource()
    {
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        if (null == args || args.length < 1)
        {
            System.err.println("system param error, need a conf file.");
            return;
        }

        /* 检查文件是否存在 */
        if (!(new File(args[0])).exists())
        {
            System.err.println("conf file[" + args[0] + "] not exist.");
            return;
        }

        MMonServer.getInstance().runSystem(args);
    }

    @Override
    protected void srvPeriodFuncPerMinutes()
    {}
}
