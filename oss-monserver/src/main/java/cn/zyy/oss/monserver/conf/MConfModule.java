package cn.zyy.oss.monserver.conf;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.monserver.share.MConstants;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssLog;

public class MConfModule extends OssTaskFsm
{
    private static final OssLog log     = new OssLog(OssLog.LOG_MODULE_OSS);
    private MSysConfig          sysConf = null;

    public MConfModule(String confFile, int iPriority) throws Exception
    {
        super(MConstants.TASK_NO_CONF, "conf_task", iPriority);

        sysConf = new MSysConfig(confFile);
        int iRet = sysConf.resolveConfig();
        if (OssConstants.RET_OK != iRet)
        {
            throw new Exception("resolve conf_file[" + confFile + "] fail");
        }
        else
        {
            log.info("system config info as follow: \n" + sysConf.toString());
        }
    }

    @Override
    public int init()
    {
        /* 设置定时器, 每分钟更新一次配置 */
        setTimer(OssCoreConstants.TIMER01, 600);

        return MConstants.RET_OK;
    }

    @Override
    public int close()
    {
        return MConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        switch (msgId)
        {
            case OssCoreConstants.MSG_ID_TIMER01:
            {
                setTimer(OssCoreConstants.TIMER01, 600);

                break;
            }

            default:
            {
                log.error("handlerMsg: recv unknown msg, msg-id=" + msgId);
                break;
            }
        }
    }

    public MSysConfig getSysConfig()
    {
        return sysConf;
    }
}
