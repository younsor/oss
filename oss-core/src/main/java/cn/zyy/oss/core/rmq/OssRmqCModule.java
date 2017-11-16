package cn.zyy.oss.core.rmq;

import java.util.Date;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import cn.zyy.oss.core.main.OssSystem;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public abstract class OssRmqCModule extends OssTaskFsm
{
    private static final OssLog   log          = new OssLog(OssLog.LOG_MODULE_OSS);
    private DefaultMQPushConsumer consumer     = null;
    private String                rmqGroup     = null;
    private String                rmqNameSrv   = null;
    private String                rmqTopic     = null;
    private String                rmqTag       = null;
    private String                consumerName = null;

    protected OssRmqCModule(int tno, String taskName, int iPriority, String group, String nameSrv, String topic, String tag)
    {
        super(tno, taskName, iPriority);

        consumerName = taskName;

        rmqGroup = group.trim();
        rmqNameSrv = nameSrv.trim();
        rmqTopic = topic.trim();
        if (null == rmqTag || rmqTag.length() <= 0)
        {
            rmqTag = "*";
        }
        else
        {
            rmqTag = tag.trim();
        }
    }

    protected String getCsmName()
    {
        return consumerName;
    }

    protected String getCsmTag()
    {
        return rmqTag;
    }

    protected String getCsmTopic()
    {
        return rmqTopic;
    }

    protected String getCsmNameSrv()
    {
        return rmqNameSrv;
    }

    protected String getCsmGroup()
    {
        return rmqGroup;
    }

    /** RMQ消费者初始化
    * groupName: 消费组名
    * nameSrv:   RMQ的名字服务地址, 格式是"192.168.100.2:9876"
    * topic:     消费的topic
    * tag:       消费topic下的tag
    * fromWhere: 消息消费的方式
    */
    private int startCsm()
    {
        /* 开始初始化消费者节点. TODU: 以后考虑将节点的错误恢复机制 */
        consumer = new DefaultMQPushConsumer(rmqGroup);
        consumer.setNamesrvAddr(rmqNameSrv);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setInstanceName(consumerName);

        try
        {
            consumer.subscribe(rmqTopic, rmqTag);
            consumer.registerMessageListener(new MessageListenerConcurrently()
            {
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> listMsg, ConsumeConcurrentlyContext Context)
                {
                    /* 如果系统不在工作状态, 则不接收消息 */
                    if (!OssSystem.sysInWorkStatus())
                    {
                        log.info("handler msg-list later, because system not in work. broker-name=%s, queue-id=%s, topic=%s", Context.getMessageQueue().getBrokerName(), Context.getMessageQueue().getQueueId(), Context.getMessageQueue().getTopic());

                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }

                    /* 将RMQ消息处理成业务消息 */
                    int iRet = rmqMsgHandler(listMsg);
                    if (OssConstants.RET_OK != iRet)
                    {
                        log.error("handler msg-list error, broker-name=%s, queue-id=%s, topic=%s", Context.getMessageQueue().getBrokerName(), Context.getMessageQueue().getQueueId(), Context.getMessageQueue().getTopic());
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }

                    log.trace("handler msg-list success, broker-name=%s, queue-id=%s, topic=%s", Context.getMessageQueue().getBrokerName(), Context.getMessageQueue().getQueueId(), Context.getMessageQueue().getTopic());
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();

            log.info("consumer start...success. group=" + rmqGroup + ", namesrv=" + rmqNameSrv + ", topic=" + rmqTopic + ", tag=" + rmqTag);
        }
        catch (Exception e)
        {
            log.info("consumer start...exception. group=" + rmqGroup + ", namesrv=" + rmqNameSrv + ", topic=" + rmqTopic + ", tag=" + rmqTag + "\n" + OssFunc.getExceptionInfo(e));

            if (null != consumer)
            {
                consumer.shutdown();
            }
            consumer = null;

            return OssCoreConstants.RET_ERROR;
        }

        return OssCoreConstants.RET_OK;
    }

    /** RMQ消费挂起 */
    private int suspendConsumer()
    {
        if (null != consumer)
        {
            try
            {
                consumer.suspend();
            }
            catch (Throwable e)
            {
                log.error("consumer.suspend exception\n" + OssFunc.getExceptionInfo(e));
            }
        }

        return OssCoreConstants.RET_OK;
    }

    /** RMQ消费恢复 */
    private int resumeConsumer()
    {
        if (null != consumer)
        {
            try
            {
                consumer.resume();
            }
            catch (Throwable e)
            {
                log.error("consumer.resume exception\n" + OssFunc.getExceptionInfo(e));
            }
        }

        return OssCoreConstants.RET_OK;
    }

    /** RMQ消费者关闭 */
    private int closeConsumer()
    {
        if (null != consumer)
        {
            try
            {
                consumer.shutdown();
            }
            catch (Throwable e)
            {
                log.error("consumer.shutdown exception\n" + OssFunc.getExceptionInfo(e));
            }
        }
        consumer = null;

        return OssCoreConstants.RET_OK;
    }

    @Override
    protected int init()
    {
        /* 参数有效性检查 */
        if (null == rmqGroup || null == rmqNameSrv || null == rmqTopic || null == rmqTag)
        {
            log.error("init param invalid: rmqGroup[" + rmqGroup + "] rmqNameSrv[" + rmqNameSrv + "] rmqTopic[" + rmqTopic + "] rmqTag[" + rmqTag + "]");
            return OssCoreConstants.RET_ERROR;
        }

        /* 重启释放 */
        closeConsumer();

        int iRet = startCsm();
        if (OssCoreConstants.RET_OK != iRet)
        {
            return iRet;
        }

        /**重要: RMQ组建自带独立的日志模块, 如果业务系统的日志模块在RMQ组建启动之前初始化, logback相关配置会被冲掉, 需要再次重置一下 */
        if (!OssLog.resetLogConfig())
        {
            return OssCoreConstants.RET_ERROR;
        }

        suspendConsumer();
        log.info("suspend csm[" + consumerName + "]..........when first into init-status");

        setTimer(OssCoreConstants.TIMER01, 2);

        return OssCoreConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        switch (msgId)
        {
            case OssCoreConstants.MSG_ID_TIMER01:
            {
                /* 如果系统状态为工作态, 则恢复消费 */
                if (OssSystem.sysInWorkStatus())
                {
                    log.info("resume csm[" + consumerName + "]..........when system into work-status, and start read-msg");
                    resumeConsumer();

                    setTimer(OssCoreConstants.TIMER02, 10);
                }
                else
                {
                    log.info("csm[" + consumerName + "] keep suspend-status..........when system always in init-status");

                    setTimer(OssCoreConstants.TIMER01, 10);
                }
                break;
            }

            case OssCoreConstants.MSG_ID_TIMER02:
            {
                /* 定期巡检RMQ: TODO */

                setTimer(OssCoreConstants.TIMER02, 30);
                break;
            }

            default:
            {
                log.error("recv invalid msg-id[" + msgId + "] when in work-status");
                break;
            }
        }
    }

    @Override
    protected int close()
    {
        log.info("suspend-and-close consumer..........when into close-status");
        suspendConsumer();
        closeConsumer();

        /* 让剩余消息消费完, RMQ消费端直接给业务处理线程发消息
         * 只要系统一进入关闭状态, 就不会再接收、发送rmq消息了 */
        OssFunc.sleep(100);

        return OssCoreConstants.RET_OK;
    }

    protected void traceMsg(MessageExt rmqMsg)
    {
        if (!log.isTraceEnabled())
        {
            return;
        }

        String bornTime = OssFunc.TimeConvert.Date2Format(new Date(rmqMsg.getBornTimestamp()), OssCoreConstants.DATE_FORMAT_FOR_RMQ);
        String storeTime = OssFunc.TimeConvert.Date2Format(new Date(rmqMsg.getStoreTimestamp()), OssCoreConstants.DATE_FORMAT_FOR_RMQ);
        log.trace("RMQ-MSG: msgid=%s, qid=%s, qoffset=%s, born-time=%s, store-time=%s", rmqMsg.getMsgId(), rmqMsg.getQueueId(), rmqMsg.getQueueOffset(), bornTime, storeTime);
    }

    protected abstract int rmqMsgHandler(List<MessageExt> listMsg);
}
