package cn.zyy.oss.core.task;

import cn.zyy.oss.core.module.OssTimerModule;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.share.OssLog;

public abstract class OssTaskFsm extends OssTaskCsm
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    public enum FsmStatus
    {
        INIT, WORK, CLOSE, QUIT
    };

    /*************************任务自己管理的变量**************************/
    private FsmStatus curStatus = FsmStatus.INIT;

    protected OssTaskFsm(int taskNo, String taskName, int iPriority)
    {
        super(taskNo, OssCoreConstants.TASK_TYPE_FSM, taskName, iPriority);
        this.curStatus = FsmStatus.INIT;
    }

    public FsmStatus getCurStatus()
    {
        return curStatus;
    }

    public boolean isQuitStatus()
    {
        return (curStatus == FsmStatus.QUIT);
    }

    protected int setTimer(int timerNo, int iMillCount)
    {
        return OssTimerModule.setTimer(getTno(), OssTimerModule.TIMER_TYPE_REL, timerNo, iMillCount, 0);
    }

    @Override
    public int onHandler(int msgId, Object objContext)
    {
        int iRet = OssCoreConstants.RET_ERROR;

        switch (curStatus)
        {
            case INIT:
            {
                if (OssCoreConstants.MSG_ID_TASK_INIT == msgId)
                {
                    iRet = init();
                    if (OssCoreConstants.RET_OK != iRet)
                    {
                        /* 初始化工作失败, 后面继续被通知进行初始化 */
                        log.error(getTaskName() + "[" + getTno() + "] recv init-msg in init-status, init fail");
                    }
                    else
                    {
                        /* 初始化成功, 此状态表示已经准备好进行工作准备 */
                        curStatus = FsmStatus.WORK;
                        log.info(getTaskName() + "[" + getTno() + "] recv init-msg in init-status, init success and into work-status");
                    }
                }
                else if (OssCoreConstants.MSG_ID_TASK_CLOSE == msgId)
                {
                    iRet = close();
                    if (OssCoreConstants.RET_OK != iRet)
                    {
                        /* 工作退出失败, 后面继续被通知进行工作退出 */
                        curStatus = FsmStatus.CLOSE;
                        log.error(getTaskName() + "[" + getTno() + "] recv close-msg in init-status, close fail");
                    }
                    else
                    {
                        /* 工作退出成功 */
                        curStatus = FsmStatus.QUIT;
                        log.info(getTaskName() + "[" + getTno() + "] recv close-msg in init-status, close success");
                    }
                }

                break;
            }

            case WORK:
            {
                switch (msgId)
                {
                    case OssCoreConstants.MSG_ID_TASK_CLOSE:
                    {
                        iRet = close();
                        if (OssCoreConstants.RET_OK != iRet)
                        {
                            /* 工作退出失败, 后面继续被通知进行工作退出 */
                            curStatus = FsmStatus.CLOSE;
                            log.error(getTaskName() + "[" + getTno() + "] recv close-msg in work-status, close fail");
                        }
                        else
                        {
                            /* 工作退出成功 */
                            curStatus = FsmStatus.QUIT;
                            log.info(getTaskName() + "[" + getTno() + "] recv close-msg in work-status, close success");
                        }
                        break;
                    }

                    default:
                    {
                        work(msgId, objContext);
                        break;
                    }
                }

                break;
            }

            case CLOSE:
            {
                /* 如果在工作状态, 执行退出没有成功; 则会在此持续进行退出工作. */
                if (OssCoreConstants.MSG_ID_TASK_CLOSE == msgId)
                {
                    iRet = close();
                    if (OssCoreConstants.RET_OK != iRet)
                    {
                        /* 初始化工作失败, 后面继续被通知进行初始化 */
                        log.error(getTaskName() + "[" + getTno() + "] recv close-msg in close-status, close fail");
                    }
                    else
                    {
                        /* 初始化成功, 此状态表示已经准备好进行工作准备 */
                        curStatus = FsmStatus.QUIT;
                        log.info(getTaskName() + "[" + getTno() + "] recv close-msg in close-status, close success");
                    }
                }

                break;
            }
        }

        return OssCoreConstants.RET_OK;
    }

    /* 状态机屏蔽quit接口, 对应用层提供init、work、close接口 */
    protected void quit()
    {
        return;
    }

    protected abstract int init();

    protected abstract void work(int msgId, Object objContext);

    protected abstract int close();
}
