package cn.zyy.oss.core.module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public abstract class OssTmoModule extends OssTaskFsm
{
    private static final OssLog log              = new OssLog();
    public static final int     UPDATE_ID_REMOVE = 0;

    public class UpdateResult
    {
        public int    id;
        public Object data;
    }

    private class TimeNode
    {
        private ReentrantLock             lock       = new ReentrantLock();

        /* 哈希对接长度 */
        private final int                 nodeIdx;
        private final int                 hashQueLen;
        private List<Map<String, Object>> lstHashMap = Lists.newArrayList();

        private TimeNode(int nodeIdx, int queLen)
        {
            this.nodeIdx = nodeIdx;
            this.hashQueLen = queLen;

            for (int idx = 0; idx < hashQueLen; idx++)
            {
                lstHashMap.add(new HashMap<String, Object>());
            }
        }

        private int add(String key, Object record)
        {
            int tmpHashValue = key.hashCode();
            if (tmpHashValue < 0)
            {
                tmpHashValue = 0 - tmpHashValue;
            }
            int index = tmpHashValue % hashQueLen;

            try
            {
                lock.lock();

                Map<String, Object> tmpMap = lstHashMap.get(index);
                Object tmpValue = tmpMap.get(key);
                if (null == tmpValue)
                {
                    tmpMap.put(key, record);
                }
                else
                {
                    log.error("add record error, because key[" + key + "]'s record has exist.");
                    return OssConstants.RET_ERROR;
                }
            }
            finally
            {
                lock.unlock();
            }

            return OssConstants.RET_OK;
        }

        private int update(String key, Object updateData)
        {
            int tmpHashValue = key.hashCode();
            if (tmpHashValue < 0)
            {
                tmpHashValue = 0 - tmpHashValue;
            }
            int index = tmpHashValue % hashQueLen;

            UpdateResult upResult = null;
            Object updateRecord = null;
            try
            {
                lock.lock();

                Map<String, Object> tmpMap = lstHashMap.get(index);
                updateRecord = tmpMap.get(key);
                if (null == updateRecord)
                {
                    /* 该key对应的记录已经不存在了, 可能是已经超时过了 */
                    log.debug("update key[%s]'s record, but not exist in node-idx[%s]", key, nodeIdx);
                    return OssConstants.RET_OK;
                }

                /* 进行更新; 判断更新完, 是否需要移除 */
                upResult = updateHandler(nodeIdx, updateRecord, updateData);
                if (null == upResult)
                {
                    log.error("update-result=null");
                }

                /* 判断是否是更新后删除 */
                if (UPDATE_ID_REMOVE == upResult.id)
                {
                    tmpMap.remove(key);
                }
            }
            finally
            {
                lock.unlock();
            }

            updatePostHandler(nodeIdx, upResult);

            return OssConstants.RET_OK;
        }

        private List<Object> removeAll()
        {
            List<Object> lstAllData = Lists.newArrayList();
            try
            {
                lock.lock();

                for (int idx = 0; idx < hashQueLen; idx++)
                {
                    Map<String, Object> tmpMap = lstHashMap.get(idx);
                    for (Object tmpValue : tmpMap.values())
                    {
                        lstAllData.add(tmpValue);
                    }

                    tmpMap.clear();
                }
            }
            finally
            {
                lock.unlock();
            }

            return lstAllData;
        }
    };

    /* 超时队列配置信息值 */
    private int            timeAccuracy;
    private int            nodeNum;
    private int            hashQueLen;
    private List<TimeNode> lstHashTable = Lists.newArrayList();

    /* 超时队列运行信息值 */
    private long           initAccTime;                        /* 所需精度的初始化时间值; 如果是秒精度, 则是秒数; 如果是10ms精度, 则是十毫秒数 */
    private long           nextTmoPos;                         /* 下一个超时时间在时间轮盘中的绝对偏离位置 */
    private long           curAccTime;                         /* 所需精度的当前时间值 */
    private long           curPos;                             /* 当前时间在时间轮盘中的绝对偏离位置 */

    protected OssTmoModule(int taskNo, String taskName, int iPriority, int accuracy, int nodeNum, int hashQueNum)
    {
        super(taskNo, taskName, iPriority);

        this.timeAccuracy = accuracy;
        this.nodeNum = nodeNum;
        this.hashQueLen = hashQueNum;

        for (int idx = 0; idx < nodeNum; idx++)
        {
            lstHashTable.add(new TimeNode(idx, hashQueLen));
        }
    }

    private long calcDiffMillSecond2AccTime(long millSecond, long accTime)
    {
        switch (timeAccuracy)
        {
            case OssCoreConstants.TIME_ACCURACY_10_MILL_SECOND:
            {
                return accTime * 10 - millSecond;
            }

            case OssCoreConstants.TIME_ACCURACY_100_MILL_SECOND:
            {
                return accTime * 100 - millSecond;
            }

            case OssCoreConstants.TIME_ACCURACY_1_SECOND:
            {
                return accTime * 1000 - millSecond;
            }

            default:
            {
                log.error("time-accuracy(" + timeAccuracy + ") invalid");

                return OssCoreConstants.RET_ERROR;
            }
        }
    }

    private long getAccTime(long millTime)
    {
        switch (timeAccuracy)
        {
            case OssCoreConstants.TIME_ACCURACY_10_MILL_SECOND:
            {
                return millTime / 10;
            }

            case OssCoreConstants.TIME_ACCURACY_100_MILL_SECOND:
            {
                return millTime / 100;
            }

            case OssCoreConstants.TIME_ACCURACY_1_SECOND:
            {
                return millTime / 1000;
            }

            default:
            {
                log.error("time-accuracy(" + timeAccuracy + ") invalid");

                return OssCoreConstants.RET_ERROR;
            }
        }
    }

    private long getAbsPos(long accTime)
    {
        long posDiff = accTime - initAccTime;

        if (posDiff < 0)
        {
            log.error("get pos error, acc-time[" + accTime + "] < init-time[" + initAccTime + "] when time-accuracy=" + timeAccuracy);

            return OssCoreConstants.RET_ERROR;
        }

        return posDiff;
    }

    private int getNodeIdx(long absPos)
    {
        return (int) (absPos % nodeNum);
    }

    /**
     * 时间轮盘扫描
     * 实时curAccTime(所需精度的当前时间值) 与 curPos(当前时间在时间轮盘中的绝对偏离位置)
     * */
    private void scanTmoNode()
    {
        StringBuilder strBuff = new StringBuilder();
        List<Object> lstTmoObject = Lists.newArrayList();
        while (nextTmoPos <= curPos)
        {
            int tmoNodeIdx = getNodeIdx(nextTmoPos);
            List<Object> lstObject = lstHashTable.get(tmoNodeIdx).removeAll();

            strBuff.append("[" + lstObject.size() + "/" + tmoNodeIdx + ":" + nextTmoPos + "]");
            nextTmoPos++;

            if (null == lstObject || lstObject.isEmpty())
            {
                continue;
            }
            lstTmoObject.addAll(lstObject);
        }

        if (strBuff.length() > 0)
        {
            /* trace信息 */
            log.trace("tmo-info: %s", strBuff);
        }

        if (lstTmoObject.size() <= 0)
        {
            return;
        }

        tmoHandler(lstTmoObject);
    }

    @Override
    protected int init()
    {
        initAccTime = getAccTime(System.currentTimeMillis());
        nextTmoPos = 0;

        curPos = 0;
        curAccTime = initAccTime;

        setTimer(OssCoreConstants.TIMER01, 1);
        return OssCoreConstants.RET_OK;
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        if (OssCoreConstants.MSG_ID_TIMER01 == msgId)
        {
            while (true)
            {
                /* 当前系统时间 */
                long millSecond = System.currentTimeMillis();
                long accTime = getAccTime(millSecond);

                /* 更新时间轮盘当前精度时间 及 偏离位置 */
                if (curAccTime < accTime)
                {
                    curAccTime = accTime;
                    curPos = getAbsPos(accTime);
                }
                else if (curAccTime > accTime)
                {
                    /* 系统时间错误 */
                    log.error("time error, cur-acc-time[" + curAccTime + "] > sys-acc-time[" + accTime + "] when cur-sys-mill-second=" + accTime);
                }

                scanTmoNode();

                /* 超时处理之后, 获取当前时间离下一个精度时间有多少毫秒 */
                millSecond = System.currentTimeMillis();
                long diffMillSecond = calcDiffMillSecond2AccTime(millSecond, curAccTime + 1);
                if (diffMillSecond <= 0)
                {
                    /* 下一个精度时间已经过了, 则继续循环 */
                }
                else if (diffMillSecond <= 2)
                {
                    /* 离下一个精度时间不到2毫秒, 则直接sleep需要等待的时间 */
                    OssFunc.accurateSleep(diffMillSecond);
                }
                else
                {
                    /* 离下一个精度时间大于2秒 */
                    if (getCurMsgNum() <= 0)
                    {
                        /* 如果当前状态机没有消息处理, 则sleep后; 继续循环判断 */
                        OssFunc.accurateSleep(2);
                    }
                    else
                    {
                        /* 如果当前状态机有消息, 需要处理; 则设置返回定时器, 后退出处理其他消息 */
                        setTimer(OssCoreConstants.TIMER01, 2);

                        return;
                    }
                }
            }
        }
    }

    @Override
    protected int close()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public int addRecord(String key, Object record, long tmoMillTime)
    {
        long tmoAccMillTime = getAccTime(tmoMillTime);

        if (tmoAccMillTime <= curAccTime)
        {
            log.error("add record[" + key + "] has timeout, when tmo-acc-time[" + tmoAccMillTime + "] <= cur-acc-time[" + curAccTime + "]");
            return OssCoreConstants.RET_ERROR;
        }

        long pos = getAbsPos(tmoAccMillTime);
        if (pos < 0)
        {
            return OssCoreConstants.RET_ERROR;
        }

        int nodeIdx = getNodeIdx(pos);
        lstHashTable.get(nodeIdx).add(key, record);

        return nodeIdx;
    }

    public int updateRecord(int nodeIdx, String key, Object updateData)
    {
        if (nodeIdx < 0 || nodeIdx >= nodeNum)
        {
            log.error("nod-idx[" + nodeIdx + "] is invalid");
            return OssConstants.RET_ERROR;
        }

        return lstHashTable.get(nodeIdx).update(key, updateData);
    }

    protected abstract void tmoHandler(List<Object> lstTmoRecord);

    /** 更新记录（同步接口）
     * 根据UpdateResult.id的取值做进一步处理, 分两种情况: 
     * -id=0, 表示需要删除记录, 并且UpdateResult.data存放被删除的记录
     * -id取其他值时, data用于存放update-post处理需要的数据, 这些数据与记录值不能有同步关系
     * */
    protected abstract UpdateResult updateHandler(int nodeIdx, Object sessData, Object updateData);

    /** 更新后处理（异步接口）
     * */
    protected abstract void updatePostHandler(int nodeIdx, UpdateResult record);
}
