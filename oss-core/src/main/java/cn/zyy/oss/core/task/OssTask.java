package cn.zyy.oss.core.task;

import cn.zyy.oss.core.module.OssMgrModule;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public abstract class OssTask
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    public interface IOssProducer
    {
        void onEntry();
    }

    public interface IOssConsumer
    {
        public int onHandler(int msgId, Object objContext);
    }

    protected int    tno;
    protected int    taskType;
    protected String taskName;
    protected int    priority;
    protected Thread thread;
    
    /* 状态机任务 */
    protected OssTask(int taskNo, int taskType, String taskName, int iPriority)
    {
        this.tno = taskNo;
        this.taskType = taskType;
        this.taskName = taskName;

        if (iPriority < OssConstants.TASK_PRIORITY_MAX)
        {
            iPriority = OssConstants.TASK_PRIORITY_MAX;
        }
        else if (iPriority > OssConstants.TASK_PRIORITY_MIN)
        {
            iPriority = OssConstants.TASK_PRIORITY_MIN;
        }
        this.priority = iPriority;

        thread = new Thread(new Runnable()
        {
            public void run()
            {
                try
                {
                    Thread.currentThread().setName(OssTask.this.taskName);

                    /* 判断任务启动是否有效, 无效则直接退出 */
                    if (!isTaskValid())
                    {
                        log.error("task invalid, and quit");
                        return;
                    }

                    OssTask.this.taskRun();

                    log.error("task run over!");
                }
                catch (Throwable t)
                {
                    log.error("exception: " + t.getClass().getName() + ", task run over");
                    log.error(OssFunc.getExceptionInfo(t));
                }

                /* 任务推出后, 释放任务号 */
                OssMgrModule.getInstance().releaseTask(OssTask.this);
            }
        });

        /* 注册task, 为了防止死循环, oss的管理任务不需要经过这一步 */
        if (OssCoreConstants.TNO_FSM_OSS_MGR != taskNo)
        {
            int registerTno = OssMgrModule.getInstance().registerTask(this);
            if (OssCoreConstants.RET_ERROR == registerTno)
            {
                log.error("OssTask: register-task error, tno=" + tno + ", taskName=" + taskName);

                return;
            }

            /* 如果注册任务号是无效的, 则在这里赋上有效的任务号 */
            if (OssCoreConstants.TNO_INVALID == this.tno)
            {
                this.tno = registerTno;
            }
        }
    }

    public int getTno()
    {
        return tno;
    }

    public String getTaskName()
    {
        return taskName;
    }

    public int getPriority()
    {
        return priority;
    }

    protected void start()
    {
        thread.start();
    }

    public boolean isAlive()
    {
        if (null == thread)
        {
            return false;
        }

        return thread.isAlive();
    }

    public boolean isTaskValid()
    {
        return (OssCoreConstants.TNO_INVALID == tno) ? false : true;
    }
    
    protected int getSysPriorityo()
    {
        return thread.getPriority();
    }
    
    protected void setSysPriorityo(int sysPriority)
    {
        thread.setPriority(sysPriority);
    }

    protected abstract void taskRun();
}
