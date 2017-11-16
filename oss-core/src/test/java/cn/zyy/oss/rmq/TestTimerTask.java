package cn.zyy.oss.rmq;

import cn.zyy.oss.core.module.OssTimerModule;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssLog;

public class TestTimerTask extends OssTaskFsm
{
    private static final OssLog log = new OssLog(OssLog.LOG_MODULE_OSS);

    protected TestTimerTask(String taskName, int iPriority)
    {
        super(1999, taskName, 0);
    }

    @Override
    public int init()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    protected void work(int msgId, Object objContext)
    {
        // TODO Auto-generated method stub
        switch (msgId)
        {
            case OssCoreConstants.MSG_ID_TASK_INIT:
            {
                log.error("recv MSG_ID_SYS_INIT....");

                log.error("set timer3 mill_second: " + System.currentTimeMillis());
                setTimer(OssCoreConstants.TIMER03, 1);
                break;
            }

            case OssCoreConstants.MSG_ID_TIMER03:
            {
                log.error("rec timer3 mill_second: " + System.currentTimeMillis());

                log.error("set timer9 mill_second: " + System.currentTimeMillis());
                setTimer(OssCoreConstants.TIMER09, 10);
                break;
            }

            case OssCoreConstants.MSG_ID_TIMER09:
            {
                log.error("rec timer9 mill_second: " + System.currentTimeMillis());

                log.error("set timer8 mill_second: " + System.currentTimeMillis());
                setTimer(OssCoreConstants.TIMER08, 10);
                break;
            }

            case OssCoreConstants.MSG_ID_TIMER08:
            {
                log.error("rec timer8 mill_second: " + System.currentTimeMillis());

                log.error("set timer3 mill_second: " + System.currentTimeMillis());
                setTimer(OssCoreConstants.TIMER03, 10);
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        TestTimerTask testTimerTask = new TestTimerTask("TestTimerTask", 0);
        OssTimerModule.getInstance();
        testTimerTask.sendMsg(OssCoreConstants.MSG_ID_TASK_INIT, null, testTimerTask.getTno());

        while (true)
        {
            Thread.sleep(100000000);
        }
    }

    @Override
    protected int close()
    {
        // TODO Auto-generated method stub
        return 0;
    }
}
