package cn.zyy.oss.core.rmq;

import java.util.Map;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskCsmCache;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssRmqPTask extends OssTaskCsmCache
{
    private static final OssLog log          = new OssLog(OssLog.LOG_MODULE_OSS);

    private DefaultMQProducer   producer     = null;
    private String              rmqGroup     = null;
    private String              rmqNameSrv   = null;
    private String              rmqTopic     = null;
    private String              rmqTag       = null;
    private String              producerName = null;

    protected OssRmqPTask(int tno, String taskName, int iPriority, String group, String nameSrv, String topic, String tag)
    {
        super(tno, taskName, iPriority);

        producerName = taskName;

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

    public int startPdc(String dir, String filePrefix, int fileMaxSizeKB)
    {
        /* 开始初始化消费者节点. TODU: 以后考虑将节点的错误恢复机制 */
        producer = new DefaultMQProducer(rmqGroup);
        producer.setNamesrvAddr(rmqNameSrv);
        producer.setInstanceName(producerName);

        try
        {
            producer.start();
        }
        catch (Throwable e)
        {
            log.error(OssFunc.getExceptionInfo(e));

            return OssCoreConstants.RET_ERROR;
        }

        /* 启动缓存模块、及任务 */
        if (OssCoreConstants.RET_OK != startTask(OssRmqCacheData.class, dir, filePrefix, fileMaxSizeKB))
        {
            log.error("start-task fail");
            return OssCoreConstants.RET_ERROR;
        }

        log.info("start rmq-pdc[" + getTaskName() + "] success");

        return OssCoreConstants.RET_OK;
    }

    public void closePdc()
    {
        if (null != producer)
        {
            try
            {
                producer.shutdown();
            }
            catch (Throwable e)
            {
                log.error("producer.shutdown exception\n" + OssFunc.getExceptionInfo(e));
            }
        }
    }

    public int putMsg(String key, byte[] msg, Map<String, String> userProperty)
    {
        OssRmqCacheData rmqRecord = new OssRmqCacheData(key, msg, userProperty);

        return add(rmqRecord);
    }

    @Override
    public int onHandler(int msgId, Object objContext)
    {
        if (!(objContext instanceof OssRmqCacheData))
        {
            log.error("objContext[" + objContext.getClass().getName() + "] not instanceof RmqCacheData");
            return OssCoreConstants.RET_ERROR;
        }
        OssRmqCacheData rmqRecord = (OssRmqCacheData) objContext;

        Message msg = new Message(rmqTopic, "", rmqRecord.key, rmqRecord.msg);

        if (null != rmqRecord.userProperty && rmqRecord.userProperty.size() > 0)
        {
            for (String propertyKey : rmqRecord.userProperty.keySet())
            {
                msg.putUserProperty(propertyKey, rmqRecord.userProperty.get(propertyKey));
            }
        }

        SendResult result = null;
        try
        {
            result = producer.send(msg);
        }
        catch (Exception e)
        {
            log.error("send rmq-msg exception: \n" + OssFunc.getExceptionInfo(e));
            return OssTaskCsmCache.RET_STOP;
        }

        log.trace("id:" + result.getMsgId() + " result:" + result.getSendStatus());
        return OssCoreConstants.RET_OK;
    }
}
