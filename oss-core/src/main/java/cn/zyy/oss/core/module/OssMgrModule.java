package cn.zyy.oss.core.module;

import java.util.ArrayList;
import java.util.List;

import cn.zyy.oss.core.main.OssSystem;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTask;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssMgrModule extends OssTaskFsm
{
    private static final OssLog log                                    = new OssLog(OssLog.LOG_MODULE_OSS);
    private static OssMgrModule ossMgrTask                             = null;
    private static int          sysStatus                              = OssCoreConstants.SYS_STATUS_INIT;

    private static final int    TIMERNO_SRV_INIT_CHECK                 = OssCoreConstants.TIMER01;
    private static final int    TIMER_MSG_SRV_INIT_CHECK               = OssCoreConstants.MSG_ID_TIMER01;
    private static final int    INTERVAL_TIME_SRV_INIT_CHECK           = 2;
    private static final long   INTERVAL_MILL_DURATION_FOR_SRV_REINIT  = 5000;

    private static final int    TIMERNO_SRV_WORK_CHECK                 = OssCoreConstants.TIMER02;
    private static final int    TIMER_MSG_SRV_WORK_CHECK               = OssCoreConstants.MSG_ID_TIMER02;
    private static final int    INTERVAL_TIME_SRV_WORK_CHECK           = 300;

    private static final long   INTERVAL_MILL_DURATION_FOR_SRV_RECLOSE = 5000;
    private static final long   INTERVAL_MILL_DURATION_FOR_SRV_KILL    = 30000;

    private class TaskMgrInfo
    {
        public int     initMsgNum = 0;
        public int     quitMsgNum = 0;
        public OssTask ossTask    = null;
    };

    public static OssMgrModule getInstance()
    {
        if (null == ossMgrTask)
        {
            ossMgrTask = new OssMgrModule();
        }

        return ossMgrTask;
    }

    public static int getSysStatus()
    {
        return sysStatus;
    }

    private List<OssTask> lstSysRegTask = new ArrayList<OssTask>();

    /* 在按顺序初始化时, 指示当前正在初始化的任务优先级 */
    private long          initMillTime;
    private int           initPriority;
    private long          quitMillTime  = 0;
    private int           quitPriority  = OssConstants.TASK_PRIORITY_MIN;

    public OssMgrModule()
    {
        super(OssCoreConstants.TNO_FSM_OSS_MGR, "oss-mgr", 0);

        /* 系统任务注册表初始化, 全部置空 */
        for (int idx = 0; idx <= OssCoreConstants.TNO_OVER; idx++)
        {
            lstSysRegTask.add(null);
        }

        registerTask(this);
    }

    /* 如果任务号无效, 则动态分配一个任务号 */
    public int registerTask(OssTask task)
    {
        if (null == task)
        {
            log.error("register consumer-task: invalid value, task=null");
            return OssCoreConstants.RET_ERROR;
        }
        int taskNo = task.getTno();

        synchronized (lstSysRegTask)
        {
            if (taskNo >= OssCoreConstants.TNO_FSM_START && taskNo <= OssCoreConstants.TNO_FSM_END)
            {
                /* 如果是指定任务号, 则必须是状态机任务号范围之内; 判断该tno位置是否已被占用 */
                OssTask tmpTask = lstSysRegTask.get(taskNo);
                if (null == tmpTask)
                {
                    lstSysRegTask.set(taskNo, task);
                }
                else
                {
                    log.error("register-task: fail (" + tmpTask.getTaskName() + ") has exist on tno(" + taskNo + "), current task(" + task.getTaskName() + ")");
                    return OssCoreConstants.RET_ERROR;
                }
            }
            else
            {
                /* 其他任务号段都需要动态分配, 此时tno应该为OssTaskConstants.TNO_INVALID */
                if (taskNo != OssCoreConstants.TNO_INVALID)
                {
                    log.error("task-tno[" + taskNo + "] need to allot, but its value != TNO_INVALID");
                    return OssCoreConstants.RET_ERROR;
                }

                for (taskNo = OssCoreConstants.TNO_TASK_START; taskNo <= OssCoreConstants.TNO_TASK_END; taskNo++)
                {
                    if (null == lstSysRegTask.get(taskNo))
                    {
                        break;
                    }
                }

                if (taskNo > OssCoreConstants.TNO_TASK_END)
                {
                    /* 所有的消费者任务号, 都没用完了, 报错 */
                    log.error("registe task[" + task.getTaskName() + "] fail, no valid tno exist");
                    return OssCoreConstants.RET_ERROR;
                }

                lstSysRegTask.set(taskNo, task);
            }
        }

        log.info("register task[" + task.getTaskName() + "] tno[" + taskNo + "] in system.");

        return taskNo;
    }

    public int releaseTask(OssTask task)
    {
        if (null == task)
        {
            log.error("register consumer-task: invalid value, task=null");
            return OssCoreConstants.RET_ERROR;
        }
        int taskNo = task.getTno();

        synchronized (lstSysRegTask)
        {
            /* 如果任务号有效, 则释放 */
            if (taskNo >= OssCoreConstants.TNO_START && taskNo <= OssCoreConstants.TNO_OVER)
            {
                lstSysRegTask.set(taskNo, null);

                log.info("release task[" + task.getTaskName() + "] tno[" + taskNo + "] in system.");
            }
        }

        return taskNo;
    }

    public OssTask getTask(int tno)
    {
        if (tno < OssCoreConstants.TNO_START || tno > OssCoreConstants.TNO_OVER)
        {
            log.error("getTask: invalid value, tmpTno=" + tno);
            return null;
        }

        return lstSysRegTask.get(tno);
    }

    /* 对特定优先级的任务, 进行初始化工作 */
    private void initSrvTasks(int priority, boolean isFirstInit)
    {
        for (int startTno = OssCoreConstants.TNO_FSM_CORE_START; startTno <= OssCoreConstants.TNO_FSM_END; startTno++)
        {
            if (null == lstSysRegTask.get(startTno))
            {
                continue;
            }

            OssTask tmpTask = lstSysRegTask.get(startTno);
            if (null == tmpTask)
            {
                log.error("init-srv-tasks: lstSysRegTask.get(" + startTno + ").ossTask is null");
                continue;
            }

            if (!(tmpTask instanceof OssTaskFsm))
            {
                log.error("init-srv-tasks: lstSysRegTask.get(" + startTno + ").ossTask not instanceof OssTaskFsm");
                continue;
            }
            OssTaskFsm tmpFsm = (OssTaskFsm) tmpTask;

            if (priority != tmpFsm.getPriority())
            {
                continue;
            }

            /* 如果任务已经不再初始化状态, 则不需要再次初始化 */
            if (FsmStatus.WORK == tmpFsm.getCurStatus())
            {
                continue;
            }

            int iRet = tmpFsm.sendMsg(OssCoreConstants.MSG_ID_TASK_INIT, null, tmpFsm.getTno());
            if (OssCoreConstants.RET_OK != iRet)
            {
                /* 反正消息发送失败, 后面会有重发机制 */
                log.info("send init-msg to " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "] error " + ((!isFirstInit) ? "again" : ""));
            }
            else
            {
                log.info("send init-msg to " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "] success " + ((!isFirstInit) ? "again" : ""));
            }
        }
    }

    /* 对特定优先级的任务, 进行初始化工作 */
    private void closeSrvTasks(int priority, boolean isFirstClose)
    {
        for (int startTno = OssCoreConstants.TNO_FSM_CORE_START; startTno <= OssCoreConstants.TNO_FSM_END; startTno++)
        {
            if (null == lstSysRegTask.get(startTno))
            {
                continue;
            }

            OssTask tmpTask = lstSysRegTask.get(startTno);
            if (null == tmpTask)
            {
                log.error("close-srv-tasks.get(" + startTno + ").ossTask is null");
                continue;
            }

            if (!(tmpTask instanceof OssTaskFsm))
            {
                log.error("close-srv-tasks: lstSysRegTask.get(" + startTno + ").ossTask not instanceof OssTaskFsm");
                continue;
            }
            OssTaskFsm tmpFsm = (OssTaskFsm) tmpTask;

            if (priority != tmpFsm.getPriority())
            {
                continue;
            }

            /* 如果任务已经不再初始化状态, 则不需要再次初始化 */
            if (FsmStatus.QUIT == tmpFsm.getCurStatus())
            {
                continue;
            }

            int iRet = tmpFsm.sendMsg(OssCoreConstants.MSG_ID_TASK_CLOSE, null, tmpFsm.getTno());
            if (OssCoreConstants.RET_OK != iRet)
            {
                /* 反正消息发送失败, 后面会有重发机制 */
                log.info("send close-msg to " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "] fail " + ((!isFirstClose) ? "again" : ""));
            }
            else
            {
                log.info("send close-msg to " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "] success " + ((!isFirstClose) ? "again" : ""));
            }
        }
    }

    /**
     * 函数扫描所有优先级为priority的任务状态
     * 所有优先级为priority的任务，状态都是taskStatus时, 才返回成功
     * 没有该优先级的任务时, 当做该优先级扫描成功
     * */
    private int checkSrvTaskStatus(int priority, FsmStatus taskStatus)
    {
        int iRet = OssCoreConstants.RET_OK;
        for (int startTno = OssCoreConstants.TNO_FSM_CORE_START; startTno <= OssCoreConstants.TNO_FSM_END; startTno++)
        {
            if (null == lstSysRegTask.get(startTno))
            {
                continue;
            }

            OssTask tmpTask = lstSysRegTask.get(startTno);
            if (null == tmpTask)
            {
                log.error("checkSrvTaskStatus: lstSysRegTask.get(" + startTno + ").ossTask is null");
                continue;
            }

            if (!(tmpTask instanceof OssTaskFsm))
            {
                log.error("close-srv-tasks: lstSysRegTask.get(" + startTno + ").ossTask not instanceof OssTaskFsm");
                continue;
            }
            OssTaskFsm tmpFsm = (OssTaskFsm) tmpTask;

            if (priority != tmpFsm.getPriority())
            {
                continue;
            }

            if (FsmStatus.QUIT == taskStatus && !tmpFsm.isAlive())
            {
                log.error("check " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "]'s status QUIT ot not, but fsm has died");
                continue;
            }

            if (taskStatus != tmpFsm.getCurStatus())
            {
                log.trace("check " + tmpFsm.getTaskName() + "[" + tmpFsm.getTno() + "]'s can not into " + taskStatus.toString());

                iRet = OssCoreConstants.RET_ERROR;
            }
        }

        return iRet;
    }

    private String getSrvTaskMsgInfo(int priority)
    {
        StringBuilder msgInfo = new StringBuilder();

        for (int startTno = OssCoreConstants.TNO_FSM_CORE_START; startTno <= OssCoreConstants.TNO_FSM_END; startTno++)
        {
            if (null == lstSysRegTask.get(startTno))
            {
                continue;
            }

            OssTask tmpTask = lstSysRegTask.get(startTno);
            if (null == tmpTask)
            {
                log.error("check-srv-fsm-status: lstSysRegTask.get(" + startTno + ").ossTask is null");
                continue;
            }

            if (!(tmpTask instanceof OssTaskFsm))
            {
                log.error("check-srv-fsm-status: lstSysRegTask.get(" + startTno + ").ossTask not instanceof OssTaskFsm");
                continue;
            }
            OssTaskFsm tmpFsm = (OssTaskFsm) tmpTask;

            if (priority != tmpFsm.getPriority())
            {
                continue;
            }

            msgInfo.append("\n\t" + tmpFsm.getTaskName() + "[ " + tmpFsm.getTno() + " ] has " + tmpFsm.getCurMsgNum() + " msg need to handler.");
        }

        return msgInfo.toString();
    }

    @Override
    public int init()
    {
        initMillTime = System.currentTimeMillis();

        setTimer(TIMERNO_SRV_INIT_CHECK, INTERVAL_TIME_SRV_INIT_CHECK);

        setTimer(TIMERNO_SRV_WORK_CHECK, INTERVAL_TIME_SRV_WORK_CHECK);

        log.info("init success. and start init all-srv-tasks");

        /* 第一次启动业务系统初始化 */
        initPriority = OssConstants.TASK_PRIORITY_MAX;
        initSrvTasks(initPriority, true);

        OssSystem.setRunInfo("priority[" + initPriority + "] tasks init");
        return OssCoreConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        long curMillTime = System.currentTimeMillis();
        switch (msgId)
        {
            case TIMER_MSG_SRV_INIT_CHECK:
            {
                if (OssCoreConstants.RET_OK != checkSrvTaskStatus(initPriority, FsmStatus.WORK))
                {
                    if (curMillTime - initMillTime > INTERVAL_MILL_DURATION_FOR_SRV_REINIT)
                    {
                        /* 如果初始化状态持续了一段时间, 则重新发送一轮初始化请求 */
                        initSrvTasks(initPriority, false);

                        initMillTime = curMillTime;
                    }

                    /* 否则还继续等待Task初始化工作结束*/
                    setTimer(TIMERNO_SRV_INIT_CHECK, INTERVAL_TIME_SRV_INIT_CHECK);
                }
                else
                {
                    if (OssConstants.TASK_PRIORITY_MIN == initPriority)
                    {
                        /* 所有级别的任务都初始化成功, 系统进入工作状态 */
                        OssSystem.setRunInfo(OssCoreConstants.RUN_INFO_INIT_OK);

                        log.info("..............system into work...............");

                        /* 设置系统状态 */
                        sysStatus = OssCoreConstants.SYS_STATUS_WORK;

                        setTimer(TIMERNO_SRV_WORK_CHECK, INTERVAL_TIME_SRV_WORK_CHECK);
                    }
                    else
                    {
                        /* 当前级别的任务初始化成功后, 启动下一个优先级的任务初始化 */
                        initPriority++;
                        initMillTime = curMillTime;

                        OssSystem.setRunInfo("priority[" + initPriority + "] tasks init");

                        log.info("all-priority[" + (initPriority - 1) + "] task has init OK, and start to init priority[" + initPriority + "] tasks");
                        initSrvTasks(initPriority, true);

                        setTimer(TIMERNO_SRV_INIT_CHECK, INTERVAL_TIME_SRV_INIT_CHECK);
                    }
                }

                break;
            }

            case TIMER_MSG_SRV_WORK_CHECK:
            {
                log.info("system working checking per %ss", INTERVAL_TIME_SRV_WORK_CHECK / 10);
                break;
            }
        }
    }

    @Override
    protected int close()
    {
        log.info("..............system start quit...............");
        sysStatus = OssCoreConstants.SYS_STATUS_CLOSE;

        /* 等待1秒钟, 保证min_priority_task没有新业务消息接收; 向其发送的close消息是最后一条 */
        OssFunc.sleep(1000);

        /* 记录退出进程信息 */
        quitMillTime = System.currentTimeMillis();
        quitPriority = OssConstants.TASK_PRIORITY_MIN;

        OssSystem.setRunInfo("priority[" + quitPriority + "] tasks close");

        /* 按照优先级由低到高, 发送close消息 */
        closeSrvTasks(quitPriority, true);
        while (true)
        {
            long curMillTime = System.currentTimeMillis();
            if (OssCoreConstants.RET_OK != checkSrvTaskStatus(quitPriority, FsmStatus.QUIT))
            {
                if (curMillTime - quitMillTime > INTERVAL_MILL_DURATION_FOR_SRV_KILL)
                {
                    /* 如果很长时间都没有退出成功, 则强制略过 */
                    log.warn("priority[" + quitPriority + "]-tasks not all-close-ok for %s seconds, and wait", INTERVAL_MILL_DURATION_FOR_SRV_KILL / 1000);
                }
                else
                {
                    if (curMillTime - quitMillTime > INTERVAL_MILL_DURATION_FOR_SRV_RECLOSE)
                    {
                        /* 如果关闭状态持续了一段时间, 则重新发送一轮关闭消息 */
                        log.info("priority[" + quitPriority + "]-tasks not all-close-ok, info as follow:" + getSrvTaskMsgInfo(quitPriority));

                        closeSrvTasks(quitPriority, false);

                        quitMillTime = curMillTime;
                    }

                    /* 继续检查各业务线程退出状态 */
                    OssFunc.sleep(500);

                    continue;
                }
            }

            if (OssConstants.TASK_PRIORITY_MAX == quitPriority)
            {
                OssSystem.setRunInfo(OssCoreConstants.RUN_INFO_QUIT_OK);

                /* 所有级别的任务都初始化成功, 系统进入工作状态 */
                log.info("all-priority[" + quitPriority + "] task has close OK");

                /* 设置系统状态后, 退出 */
                sysStatus = OssCoreConstants.SYS_STATUS_EXIT;
                break;
            }
            else
            {
                /* 当前级别的任务初始化成功后, 启动下一个优先级的任务初始化 */
                quitPriority--;
                quitMillTime = curMillTime;

                OssSystem.setRunInfo("priority[" + quitPriority + "] tasks close");

                log.info("all-priority[" + quitPriority + "] task has close OK, and start to close priority[" + (quitPriority - 1) + "] tasks");

                closeSrvTasks(quitPriority, true);

                OssFunc.sleep(100);
            }
        }

        return OssCoreConstants.RET_OK;
    }
}
