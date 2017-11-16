package cn.zyy.oss.core.task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import cn.zyy.oss.core.module.OssMgrModule;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.share.OssMsg;
import cn.zyy.oss.core.task.OssTask.IOssConsumer;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public abstract class OssTaskCsm extends OssTask implements IOssConsumer
{
    private static final OssLog log                      = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int    MAX_MSG_QUEUE_LENGTH     = 20000;
    private static final int    MAX_LCS_MSG_QUEUE_LENGTH = 18000;

    /*************************任务自己管理的变量**************************/
    private BlockingQueue       msgQueue                 = null;
    private int                 curSender;

    protected OssTaskCsm(int taskNo, int taskType, String taskName, int iPriority)
    {
        super(taskNo, taskType, taskName, iPriority);
        this.taskType = taskType;

        msgQueue = new ArrayBlockingQueue<OssMsg>(MAX_MSG_QUEUE_LENGTH);

        start();
    }

    protected OssTaskCsm(int taskNo, String taskName, int iPriority)
    {
        super(taskNo, OssCoreConstants.TASK_TYPE_CONSUMER, taskName, iPriority);
        msgQueue = new ArrayBlockingQueue<OssMsg>(MAX_MSG_QUEUE_LENGTH);

        start();
    }

    /* 消费者任务的线程入口 */
    protected void taskRun()
    {
        OssMsg msg = null;
        while (true)
        {
            try
            {
                msg = (OssMsg) msgQueue.take();
            }
            catch (Throwable t)
            {
                log.error("msgQueue.take exception: " + t.getClass().getName());
                log.error(OssFunc.getExceptionInfo(t));
                continue;
            }

            if (null == msg)
            {
                continue;
            }
            curSender = msg.srcTno;

            /* 如果是杀任务的消息, 则任务跳出, 线程自动退出 */
            if (OssCoreConstants.MSG_ID_TASK_KILL == msg.msgId)
            {
                try
                {
                    quit();
                }
                catch (Throwable t)
                {
                    log.error("task[" + getTno() + "-" + getTaskName() + "] quit exception\n" + OssFunc.getExceptionInfo(t));
                }

                String srcTaskName = "null";
                if (null != OssMgrModule.getInstance().getTask(msg.srcTno))
                {
                    srcTaskName = OssMgrModule.getInstance().getTask(msg.srcTno).getTaskName();
                }
                log.info("recv KILL msg from " + srcTaskName + ", and task[" + getTno() + "-" + getTaskName() + "] quit, " + getCurMsgNum() + " msg not handler");

                break;
            }

            try
            {
                /* 如果是系统系统消息, 则 */
                onHandler(msg.msgId, msg.objContext);
            }
            catch (Throwable t)
            {
                log.error("handler msg[" + msg.msgId + "] exception: " + t.getClass().getName());
                log.error(OssFunc.getExceptionInfo(t));
            }
        }
    }

    public int getSender()
    {
        return curSender;
    }

    public int getCurMsgNum()
    {
        if (null == msgQueue)
        {
            return 0;
        }

        return msgQueue.size();
    }

    public String taskInfo()
    {
        return "cur-msg=" + getCurMsgNum() + ", ";
    }

    protected OssMsg getNextMsgNotRemove()
    {
        return (OssMsg) msgQueue.peek();
    }

    protected OssMsg getNextMsgAndRemove()
    {
        return (OssMsg) msgQueue.poll();
    }

    public int sendMsgEx(int msgId, Object objMsg, int destTno, int srcTno)
    {
        OssTask destTask = OssMgrModule.getInstance().getTask(destTno);
        if (null == destTask)
        {
            log.error("send-msg-ex[" + msgId + "] to task[" + destTno + "] fail, because dest-task is null");
            return OssCoreConstants.RET_ERROR;
        }

        if (!(destTask instanceof OssTaskCsm))
        {
            log.error("send-msg-ex[" + msgId + "] to task[" + destTno + "] fail, because dest-task not instance OssTaskConsumer");
            return OssCoreConstants.RET_ERROR;
        }
        OssTaskCsm consumerTask = (OssTaskCsm) destTask;

        int queLen = consumerTask.msgQueue.size();
        if (queLen >= MAX_MSG_QUEUE_LENGTH)
        {
            return OssCoreConstants.RET_OSS_MSG_QUEUE_FULL;
        }

        OssMsg msg = new OssMsg();
        msg.srcTno = srcTno;
        msg.destTno = destTno;
        msg.msgId = msgId;
        msg.objContext = objMsg;

        if (true != consumerTask.msgQueue.offer(msg))
        {
            return OssCoreConstants.RET_OSS_MSG_QUEUE_ERROR;
        }

        return OssCoreConstants.RET_OK;
    }

    /** 负荷控制等待(load control sleep)消息接口
     *  如果消息接收任务负荷过重, 达到一定限度后, 则主动控制消息发送的速度:
     *  每发送一个消息, 主动延迟millSecond毫秒
     * */
    public int sendMsgLcsEx(int msgId, Object objMsg, int destTno, int srcTno, int millSecond)
    {
        OssTask destTask = OssMgrModule.getInstance().getTask(destTno);
        if (null == destTask)
        {
            log.error("send-msg-lcs-ex[" + msgId + "] to task[" + destTno + "] fail, because dest-task is null");
            return OssCoreConstants.RET_ERROR;
        }

        if (!(destTask instanceof OssTaskCsm))
        {
            log.error("send-msg-lcs-ex[" + msgId + "] to task[" + destTno + "] fail, because dest-task not instance OssTaskConsumer");
            return OssCoreConstants.RET_ERROR;
        }
        OssTaskCsm consumerTask = (OssTaskCsm) destTask;

        int queLen = consumerTask.msgQueue.size();
        if (queLen >= MAX_LCS_MSG_QUEUE_LENGTH)
        {
            if (millSecond <= 0)
            {
                millSecond = 1;
            }
            OssFunc.sleep(millSecond);
        }

        if (queLen >= MAX_MSG_QUEUE_LENGTH - 2)
        {
            queLen = consumerTask.msgQueue.size();
            if (queLen >= MAX_MSG_QUEUE_LENGTH)
            {
                return OssCoreConstants.RET_OSS_MSG_QUEUE_FULL;
            }
        }

        OssMsg msg = new OssMsg();
        msg.srcTno = srcTno;
        msg.destTno = destTno;
        msg.msgId = msgId;
        msg.objContext = objMsg;

        String srcTaskName = OssMgrModule.getInstance().getTask(srcTno).getTaskName();
        String destTaskName = OssMgrModule.getInstance().getTask(destTno).getTaskName();
        if (true != consumerTask.msgQueue.offer(msg))
        {
            log.error("src_task_tno[" + srcTno + "] send msg[" + msgId + "] to dest_task_tno[ " + destTno + " ] fail");
            return OssCoreConstants.RET_OSS_MSG_QUEUE_ERROR;
        }
        else
        {
            log.trace("src_task[" + srcTaskName + "] send msg[" + msgId + "] to dest_task[ " + destTaskName + " ] success");
        }

        return OssCoreConstants.RET_OK;
    }

    public int sendMsg(int msgId, Object objMsg, int destTno)
    {
        int srcTno = getTno();
        return sendMsgEx(msgId, objMsg, destTno, srcTno);
    }

    protected int sendMsgLcs(int msgId, Object objMsg, int destTno, int millSecond)
    {
        int srcTno = getTno();
        return sendMsgLcsEx(msgId, objMsg, destTno, srcTno, millSecond);
    }

    protected abstract void quit();
}
