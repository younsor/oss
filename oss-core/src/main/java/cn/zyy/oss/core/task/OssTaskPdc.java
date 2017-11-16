package cn.zyy.oss.core.task;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTask.IOssProducer;
import cn.zyy.oss.share.OssLog;

public abstract class OssTaskPdc extends OssTask implements IOssProducer
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    /*************************任务自己管理的变量**************************/
    protected OssTaskPdc(int taskNo, String taskName, int iPriority)
    {
        super(taskNo, OssCoreConstants.TASK_TYPE_PRODUCER, taskName, iPriority);
    }

    protected void taskRun()
    {
        onEntry();
    }
}
