package cn.zyy.oss.core.rmq;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssRmqPModule extends OssTaskFsm
{
    private static final OssLog log          = new OssLog(OssLog.LOG_MODULE_OSS);
    private String              rmqGroup     = null;
    private String              rmqNameSrv   = null;
    private String              rmqTopic     = null;
    private String              rmqTag       = null;
    private String              producerName = null;

    private int                 pdvNum       = 0;
    private List<OssRmqPTask>   lstPdcTask   = Lists.newArrayList();

    private String              cacheDir;

    private long                msgNum       = 0;

    public OssRmqPModule(int tno, String taskName, int iPriority, int num, String group, String nameSrv, String topic, String tag, String dir)
    {
        super(tno, taskName, iPriority);

        pdvNum = num;

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

        cacheDir = dir;
    }

    @Override
    protected int init()
    {
        /* 参数有效性检查 */
        if (pdvNum <= 0 || null == rmqGroup || null == rmqNameSrv || null == rmqTopic || null == rmqTag)
        {
            log.error("init param invalid: pdvNum[" + pdvNum + "] rmqGroup[" + rmqGroup + "] rmqNameSrv[" + rmqNameSrv + "] rmqTopic[" + rmqTopic + "] rmqTag[" + rmqTag + "]");
            return OssCoreConstants.RET_ERROR;
        }

        /* 释放 */
        for (int idx = 0; idx < lstPdcTask.size(); idx++)
        {
            OssRmqPTask tmpPdcTask = lstPdcTask.get(idx);
            if (null != tmpPdcTask)
            {
                tmpPdcTask.closePdc();
                lstPdcTask.set(idx, null);
            }
        }

        /* 创建num个Producer */
        for (int idx = 0; idx < pdvNum; idx++)
        {
            String taskName = producerName + "-" + idx;
            OssRmqPTask tmpPdcTask = new OssRmqPTask(OssCoreConstants.TNO_INVALID, taskName, getPriority(), rmqGroup, rmqNameSrv, rmqTopic, rmqTag);
            if (OssCoreConstants.RET_OK != tmpPdcTask.startPdc(cacheDir, "pdc-cache", 50 * 1024))
            {
                log.error("start " + idx + " pdc-task error");
                return OssCoreConstants.RET_ERROR;
            }

            lstPdcTask.add(tmpPdcTask);
        }

        /**重要: RMQ组建自带独立的日志模块, 如果业务系统的日志模块在RMQ组建启动之前初始化, logback相关配置会被冲掉, 需要再次重置一下 */
        if (!OssLog.resetLogConfig())
        {
            return OssCoreConstants.RET_ERROR;
        }

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
                /* 定期巡检RMQ: TODO */

                setTimer(OssCoreConstants.TIMER01, 30);
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
        /* 释放 */
        for (int idx = 0; idx < lstPdcTask.size(); idx++)
        {
            OssRmqPTask tmpPdcTask = lstPdcTask.get(idx);
            if (null != tmpPdcTask)
            {
                tmpPdcTask.closePdc();
                lstPdcTask.set(idx, null);
            }
        }
        lstPdcTask.clear();

        /* 让剩余消息消费完, RMQ消费端直接给业务处理线程发消息
         * 只要系统一进入关闭状态, 就不会再接收、发送rmq消息了 */
        OssFunc.sleep(100);

        return OssCoreConstants.RET_OK;
    }

    public int addMsg(String key, byte[] msg, Map<String, String> userProperty)
    {
        long tmpMsgNum = msgNum++;
        int idx = (int) (tmpMsgNum % pdvNum);
        return lstPdcTask.get(idx).putMsg(key, msg, userProperty);
    }
}
