package cn.zyy.oss.core.module;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.httpkit.client.IResponseHandler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cn.zyy.oss.core.main.OssSystem;
import cn.zyy.oss.core.proto.SysMonitor;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.share.OssMon;
import cn.zyy.oss.core.share.OssMon.MonInfo;
import cn.zyy.oss.core.share.OssMon.MonNode;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.http.OssClient;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssDefine;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;
import cn.zyy.oss.share.OssRequest;

public class OssMonModule extends OssTaskFsm
{
    private static final OssLog  log                          = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int     DELAY_HANDLER_SECONDS        = 5;
    private static final int     WHEEL_NODE_NUM               = 1800;

    private static final int     TIMER_MONINFO_COLLECT        = OssCoreConstants.TIMER01;
    private static final int     MSG_ID_TIMER_MONINFO_COLLECT = OssCoreConstants.MSG_ID_TIMER01;
    private static final int     INTERIM_MONINFO_COLLECT      = 2;

    private static final int     TIMER_MONINFO_SEND           = OssCoreConstants.TIMER02;
    private static final int     MSG_ID_TIMER_MONINFO_SEND    = OssCoreConstants.MSG_ID_TIMER02;
    private static final int     INTERIM_MONINFO_SEND         = 1;

    private static OssMonModule  ossMonTask                   = null;
    private static ReentrantLock sysMonlock                   = new ReentrantLock();
    private static List<MonInfo> lstSysMonInfo                = Lists.newArrayList();

    /** 监控信息起始时间(sysMonStartSecond): 用于定位应用层监控信息时间的偏移量
     *  监控信息收集时间(sysMonCollectSecond): 用于记录当前收集的时间
     *  一个用于应用层写监控信息, 一个用于支撑曾读监控信息. 支撑层读性能数据时, 会延迟几秒, 足以错开读、写操作 */
    private static long          sysMonStartSecond            = 0;
    private static long          sysMonCollectSecond          = 0;

    public static long getSysMonStartSecond()
    {
        return sysMonStartSecond;
    }

    public static long getSysMonCollectSecond()
    {
        return sysMonCollectSecond;
    }

    public static int registerMonitor(String type, int secNum, int sampleNum)
    {
        int id;
        sysMonlock.lock();
        try
        {
            MonInfo monInfo = new MonInfo(type, secNum, sampleNum, getSysMonStartSecond());
            monInfo.taskName = Thread.currentThread().getName();

            lstSysMonInfo.add(monInfo);
            id = lstSysMonInfo.size() - 1;
        }
        finally
        {
            sysMonlock.unlock();
        }

        return id;
    }

    public static boolean recordMonInfo(int id, long secTime, List<Long> lstSampleInfo)
    {
        if (id >= lstSysMonInfo.size())
        {
            return false;
        }

        MonInfo monInfo = lstSysMonInfo.get(id);
        String taskName = Thread.currentThread().getName();
        if (!taskName.equals(monInfo.taskName))
        {
            return false;
        }

        try
        {
            monInfo.add(secTime, lstSampleInfo);
        }
        catch (Exception e)
        {
            log.error(OssFunc.getExceptionInfo(e));
            return false;
        }

        return true;
    }

    private class GMonClientHandler implements IResponseHandler
    {
        private long clientSendSecond = 0;

        public GMonClientHandler(long secondTime)
        {
            this.clientSendSecond = secondTime;
        }

        public void onSuccess(int status, Map<String, Object> headers, Object body)
        {
            // if (clientSendSecond != sendSecond)
            // {
            // log.error("clientSendSecond(%s) != sendSecond(%s)",
            // clientSendSecond, sendSecond);
            // }
        }

        public void onThrowable(Throwable t)
        {
            /* 服务端无法建链路的情况下, 不持续发送, 等待链路恢复
             * 其他异常都继续发送, 不能等 */
            OssFunc.getExceptionInfo(t);
        }

        public void onInnerError(Throwable t)
        {
            OssFunc.getExceptionInfo(t);
        }
    }

    public static OssMonModule getInstance()
    {
        if (null == ossMonTask)
        {
            ossMonTask = new OssMonModule(0);
        }

        return ossMonTask;
    }

    private class MonWheel
    {
        private List<Map<OssDefine.MapKey, MonNode>> lstMonStatInfo = Lists.newArrayList();

        public MonWheel()
        {
            for (int idx = 0; idx <= WHEEL_NODE_NUM; idx++)
            {
                Map<OssDefine.MapKey, MonNode> mapValue = Maps.newHashMap();
                lstMonStatInfo.add(mapValue);
            }
        }
    }

    private OssClient monClient  = null;
    private MonWheel  monWheel   = null;
    private long      sendSecond = 0;

    private OssMonModule(int iPriority)
    {
        super(OssCoreConstants.TNO_FSM_OSS_MON, "oss-monitor", iPriority);

        monWheel = new MonWheel();
    }

    @Override
    public int init()
    {
        setTimer(TIMER_MONINFO_COLLECT, INTERIM_MONINFO_COLLECT);

        /* 初始化监控信息收集时间, 此时间作为各应用MonInfo的监控信息采集起点 */
        sysMonStartSecond = System.currentTimeMillis() / 1000;
        sysMonCollectSecond = sysMonStartSecond;
        sendSecond = sysMonStartSecond;

        monClient = new OssClient("oss-monitor-client");
        if (!monClient.start())
        {
            log.info("init success. because monitor client start fail");
            return OssConstants.RET_ERROR;
        }

        setTimer(TIMER_MONINFO_SEND, INTERIM_MONINFO_SEND);

        log.info("init success. and monitor-start-second is " + sendSecond);
        return OssConstants.RET_OK;
    }

    private void collectMonInfo(long handlerSecond)
    {
        int collectIdx = (int) (handlerSecond % WHEEL_NODE_NUM);
        Map<OssDefine.MapKey, MonNode> handlerMapMonInfo = monWheel.lstMonStatInfo.get(collectIdx);

        for (MonInfo monInfo : lstSysMonInfo)
        {
            OssDefine.MapKey monKey = new OssDefine.MapKey(monInfo.type);
            monKey.put(OssCoreConstants.MON_KEY_FIELD_HOST, OssSystem.sysHostName());
            monKey.put(OssCoreConstants.MON_KEY_FIELD_SYSTEM, OssSystem.sysName());
            monKey.put(OssCoreConstants.MON_KEY_FIELD_TASK, monInfo.taskName);

            MonNode handlerMonNode = handlerMapMonInfo.get(monKey);
            if (null == handlerMonNode)
            {
                handlerMonNode = monInfo.merge2OtherNodeAndClear(handlerSecond, null);
                if (null == handlerMonNode)
                {
                    continue;
                }

                handlerMapMonInfo.put(monKey, handlerMonNode);
            }
            else
            {
                monInfo.merge2OtherNodeAndClear(handlerSecond, handlerMonNode);
            }
        }
    }

    private SysMonitor.Info getSendMonInfo(long second)
    {
        int sendIdx = (int) (second % WHEEL_NODE_NUM);
        Map<OssDefine.MapKey, MonNode> sendMapMonInfo = monWheel.lstMonStatInfo.get(sendIdx);
        if (sendMapMonInfo.size() <= 0)
        {
            return null;
        }

        SysMonitor.Info.Builder monSendInfoBuilder = SysMonitor.Info.newBuilder();
        monSendInfoBuilder.setSecondTime(second);

        for (OssDefine.MapKey mapKey : sendMapMonInfo.keySet())
        {
            SysMonitor.Perf.Builder monSendPerfBuilder = SysMonitor.Perf.newBuilder();

            SysMonitor.Key.Builder monSendKeyBuilder = SysMonitor.Key.newBuilder();
            monSendKeyBuilder.setType(mapKey.getType());
            monSendKeyBuilder.setHost(mapKey.get(OssCoreConstants.MON_KEY_FIELD_HOST));
            monSendKeyBuilder.setSystem(mapKey.get(OssCoreConstants.MON_KEY_FIELD_SYSTEM));
            monSendKeyBuilder.setTask(mapKey.get(OssCoreConstants.MON_KEY_FIELD_TASK));
            monSendPerfBuilder.setMonKey(monSendKeyBuilder);

            MonNode mapValue = sendMapMonInfo.get(mapKey);
            for (OssMon.AveValue aveValue : mapValue.getAllValue())
            {
                SysMonitor.AveValue.Builder monSendValueBuilder = SysMonitor.AveValue.newBuilder();
                monSendValueBuilder.setNum(aveValue.num());
                monSendValueBuilder.setValue(aveValue.value());
                monSendPerfBuilder.addMonValue(monSendValueBuilder);
            }

            monSendInfoBuilder.addMonPerf(monSendPerfBuilder);
        }

        return monSendInfoBuilder.build();
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        long curSecondTime = System.currentTimeMillis() / 1000;
        switch (msgId)
        {
            case MSG_ID_TIMER_MONINFO_COLLECT:
            {
                if (!OssSystem.monStatus())
                {
                    sysMonCollectSecond = curSecondTime;

                    setTimer(TIMER_MONINFO_COLLECT, 50);
                    return;
                }

                /* 监控信息收集时间 需要比当前时间延迟 DELAY_HANDLER_SECONDS 秒 */
                if (sysMonCollectSecond + DELAY_HANDLER_SECONDS >= curSecondTime)
                {
                    setTimer(TIMER_MONINFO_COLLECT, INTERIM_MONINFO_COLLECT);
                    return;
                }

                /* 异常判断, 当前收集时间不能超过发送时间一轮; 若超一轮, 肯定是监控信息未上报成功导致, 此时, 覆盖处理 */
                if (sendSecond + WHEEL_NODE_NUM <= sysMonCollectSecond)
                {
                    sendSecond++;
                    log.error("system monitor info report fail, and %s's info need to be delete", sendSecond);
                }

                collectMonInfo(sysMonCollectSecond);
                sysMonCollectSecond++;

                setTimer(TIMER_MONINFO_COLLECT, INTERIM_MONINFO_COLLECT);
                break;
            }

            case MSG_ID_TIMER_MONINFO_SEND:
            {
                if (!OssSystem.monStatus())
                {
                    sendSecond = sysMonCollectSecond - 2;
                    setTimer(TIMER_MONINFO_SEND, 50);
                    return;
                }

                /* 监控消息发送的时间 不能超过 收集时间.
                 * 如果发送不成功, 就丢弃, 积攒了较长时间没有发送，则快速发送 */
                if (sendSecond >= sysMonCollectSecond - 1)
                {
                    setTimer(TIMER_MONINFO_SEND, INTERIM_MONINFO_SEND);
                    return;
                }

                SysMonitor.Info monInfo = getSendMonInfo(sendSecond);
                if (null == monInfo)
                {
                    // log.debug("%s's system-monitor-info null, not send.
                    // curCollectTime=%s", sendSecond, sysMonCollectSecond);

                    /* 如果该秒系统没有监控信息, 则跳过; 可能监控功能关闭 */
                    sendSecond++;
                }
                else
                {
                    OssRequest oriReq = new OssRequest();
                    oriReq.type = OssConstants.HTTP_REQ_TYPE_POST;
                    oriReq.url = OssSystem.monServerUrl();
                    oriReq.headers.put("Content-Type", "application/octet-stream");
                    oriReq.postBody = monInfo.toByteArray();
                    monClient.sendRequest(oriReq, new GMonClientHandler(sendSecond), 200);

                    log.debug("%s's system-monitor-info is as follow, has send. curCollectTime=%s \n%s", sendSecond, sysMonCollectSecond, monInfo);

                    /* TODO 不管消息有没有发送成功，当前只要发送过就不管了, 以后完善 */
                    sendSecond++;
                }

                setTimer(TIMER_MONINFO_SEND, INTERIM_MONINFO_SEND);
                break;
            }

            default:
            {
                log.error("handlerMsg: recv unknown msg, msg-id=" + msgId);
                break;
            }
        }
    }

    @Override
    public int close()
    {
        return OssConstants.RET_OK;
    }
}
