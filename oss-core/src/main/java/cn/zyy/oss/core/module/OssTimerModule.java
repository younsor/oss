package cn.zyy.oss.core.module;

import java.util.ArrayList;
import java.util.List;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssTimerModule extends OssTaskFsm
{
    private static final OssLog   log             = new OssLog(OssLog.LOG_MODULE_OSS);

    /* 定时器管理控制块数量 */
    private static final int      TMCB_NUM        = 4000;
    private static final int      TQ_NUM          = 42;
    private static final int      UN_USE          = 0;
    private static final int      IN_USE          = 1;
    private static final int      INVALID_VALUE   = -1;

    /* 定时器类型 */
    public static final int       TIMER_TYPE_REL  = 1;                                /* 定时器类型：相对定时器 */
    public static final int       TIMER_TYPE_ABS  = 2;                                /* 定时器类型：绝对定时器 */
    public static final int       TIMER_TYPE_LOOP = 3;                                /* 定时器类型：循环定时器 */

    /* 定时器任务是单例 */
    private static OssTimerModule ossTimerTask    = null;

    public static OssTimerModule getInstance()
    {
        if (null == ossTimerTask)
        {
            ossTimerTask = new OssTimerModule();
        }

        return ossTimerTask;
    }

    private class TimerQueue
    {
        public int iCount;   /* 定时级别 ; 如queue[19]的定时级别时9秒*/
        public int iRest;    /* 队列剩余时长(所有节点都移除所需要经历的时间) */
        public int iTmcbNum; /* 队列中TMCB链表节点个数 */
        public int iHead;    /* 队列的TMCB链表头节点 */
        public int iTail;    /* 队列的TMCB链表尾节点 */
    }

    private class TimerControlBlock
    {
        public int iUse;     /* 空闲标志 */
        public int iType;    /* 定时器类型 */
        public int iQue;     /* 定时器当前所在队列号 */
        public int iTimerNo; /* 定时器编号 */
        public int iCount;   /* 定时器时长; 相对定时器:百毫秒; 绝对定时器:秒 */
        public int iRest;    /* 剩余触发百毫秒数 */
        public int iParam;   /* 定时器参数, 回带给应用 */
        public int iTno;     /* 归属任务号 */
        public int iQue1s;   /* 1秒消息队列 */
        public int iQue10s;  /* 10秒消息队列 */
        public int iQue100s; /* 100秒消息队列 */
        public int iPrev;    /* 队列中前一个控制块索引, 双向非循环链表 */
        public int iNext;    /* 队列中后一个控制块索引, 双向非循环链表 */
    }

    private class TimerControlBlockPool
    {
        public int   iMaxUsed;   /* TMCB最大使用数 */
        public int   iIdleNum;   /* 空闲TMCB数量 */
        public int   iHead;      /* 空闲链表头位置 */
        public int   iTail;      /* 空闲链表尾的下一个位置 */
        public int[] aiIdleList; /* 空闲TMCB链表 */
    }

    private List<TimerControlBlock> lstTCB  = new ArrayList<TimerControlBlock>();
    private TimerControlBlockPool   tcbPool = new TimerControlBlockPool();
    private List<TimerQueue>        lstTq   = new ArrayList<TimerQueue>();

    protected OssTimerModule()
    {
        super(OssCoreConstants.TNO_FSM_OSS_TIMER, "oss_timer", 0);
    }

    private void initTCBPool()
    {
        /* 初始化定时器控制块 */
        for (int idx = 0; idx < TMCB_NUM; idx++)
        {
            TimerControlBlock tcb = new TimerControlBlock();
            tcb.iUse = UN_USE;
            tcb.iType = INVALID_VALUE;
            tcb.iQue = INVALID_VALUE;
            tcb.iTimerNo = INVALID_VALUE;
            tcb.iCount = 0;
            tcb.iRest = 0;
            tcb.iParam = INVALID_VALUE;
            tcb.iTno = INVALID_VALUE;
            tcb.iQue1s = INVALID_VALUE;
            tcb.iQue10s = INVALID_VALUE;
            tcb.iQue100s = INVALID_VALUE;
            tcb.iPrev = INVALID_VALUE;
            tcb.iNext = INVALID_VALUE;

            lstTCB.add(tcb);
        }

        /* 初始化定时器控制块池, 空闲链表的初始状态为满
         * wHead == wTail时: 空闲链表空, 没有可用的定时器控制块
         * wHead == wTail%(TMCB_NUM+1)时: 空闲链表满
         */
        tcbPool.iMaxUsed = 0;
        tcbPool.iIdleNum = TMCB_NUM;
        tcbPool.iHead = 0;
        tcbPool.iTail = TMCB_NUM;
        tcbPool.aiIdleList = new int[TMCB_NUM + 1];
        for (int idx = 0; idx <= TMCB_NUM; idx++)
        {
            tcbPool.aiIdleList[idx] = idx;
        }

        /* 初始化定时器队列, 各种队列用途如下: 
         * 0:     用于存放过期时间在1s内的定时器
         * 1-9:   用于分解存放 [0.1s, 0.9s] 的定时器
         * 11-19: 用于分解存放 [1s, 9s] 的定时器
         * 21-29: 用于分解存放 [10s, 90s] 的定时器
         * 31-39: 用于分解存放 [100s, 900s] 的定时器
         * 40:    用于存放 [1000s, +) 的定时器
         * 41:    用于存放绝对定时器
         */
        for (int idx = 0; idx < TQ_NUM; idx++)
        {
            TimerQueue tmpTq = new TimerQueue();
            tmpTq.iCount = 0;
            tmpTq.iRest = 0;
            tmpTq.iTmcbNum = 0;
            tmpTq.iHead = 0;
            tmpTq.iTail = 0;
            lstTq.add(tmpTq);
        }

        lstTq.get(0).iCount = 0;
        lstTq.get(0).iRest = 0;
        lstTq.get(0).iHead = INVALID_VALUE;
        lstTq.get(0).iTail = INVALID_VALUE;

        for (int idx = 1; idx <= 9; idx++)
        {
            lstTq.get(idx).iCount = idx;
            lstTq.get(idx).iRest = 0;
            lstTq.get(idx).iHead = INVALID_VALUE;
            lstTq.get(idx).iTail = INVALID_VALUE;
        }

        for (int idx = 11; idx <= 19; idx++)
        {
            lstTq.get(idx).iCount = (idx - 10) * 10;
            lstTq.get(idx).iRest = 0;
            lstTq.get(idx).iHead = INVALID_VALUE;
            lstTq.get(idx).iTail = INVALID_VALUE;
        }

        for (int idx = 21; idx <= 29; idx++)
        {
            lstTq.get(idx).iCount = (idx - 20) * 100;
            lstTq.get(idx).iRest = 0;
            lstTq.get(idx).iHead = INVALID_VALUE;
            lstTq.get(idx).iTail = INVALID_VALUE;
        }

        for (int idx = 31; idx <= 39; idx++)
        {
            lstTq.get(idx).iCount = (idx - 30) * 1000;
            lstTq.get(idx).iRest = 0;
            lstTq.get(idx).iHead = INVALID_VALUE;
            lstTq.get(idx).iTail = INVALID_VALUE;
        }

        lstTq.get(40).iCount = 0;
        lstTq.get(40).iRest = 0;
        lstTq.get(40).iHead = INVALID_VALUE;
        lstTq.get(40).iTail = INVALID_VALUE;

        lstTq.get(41).iCount = 0;
        lstTq.get(41).iRest = 0;
        lstTq.get(41).iHead = INVALID_VALUE;
        lstTq.get(41).iTail = INVALID_VALUE;
    }

    @Override
    public int init()
    {
        /* 初始化定时器池 */
        initTCBPool();

        /* 定时器任务需要自己进入工作状态, 这样系统所有Task的定时器消息才得以继续 */
        sendMsg(OssCoreConstants.MSG_ID_TASK_WORK, null, getTno());

        log.info(getTaskName() + "[" + getTno() + "] init.........");
        return OssCoreConstants.RET_OK;
    }

    @Override
    public int close()
    {
        return OssCoreConstants.RET_OK;
    }

    /** TimerTask从接收到init消息后，就进入handlerMsg中
     * 一直以百毫秒的粒度循环遍历定时器
     **/
    private void timerProc()
    {
        long nextTimeoutMillTime = System.currentTimeMillis() + 100;
        long timerNanoSec = 100000000;
        while (true)
        {
            OssFunc.nanoSleep(timerNanoSec);

            /* 判断当前时间戳是否过了100毫秒 */
            long curMillTime = System.currentTimeMillis();
            if (nextTimeoutMillTime >= curMillTime + 1)
            {
                /* 还未到定时的100ms时, 计算偏差时间, 如果偏差时间大于1ms, 则继续定时 */
                timerNanoSec = (nextTimeoutMillTime - curMillTime) * 1000000L;
                continue;
            }

            do
            {
                relTimerProc();
                // log.debug("nextTimeoutMillTime=" + nextTimeoutMillTime + ",
                // curMillTime=" + curMillTime);
                nextTimeoutMillTime += 100;

                /* 由于系统的定时器精度有差别, nanoSleep定时的时间可能超出了100ms
                 * 如果超出的时间 >99ms, 则马上启动下一轮定时器处理 */
                curMillTime = System.currentTimeMillis();
            }
            while (nextTimeoutMillTime < curMillTime + 1);

            timerNanoSec = (nextTimeoutMillTime - curMillTime) * 1000000;
            if (timerNanoSec < 0)
            {
                log.error("handlerMsg: timerNanoSec=" + timerNanoSec + " invalid");
            }
        }
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        timerProc();
    }

    /**
    * 函数名称：AddLevelTimerQue
    * 功能描述：将TMCB加入分级队列, 这些队列由100ms、1s、10s、100s构成
    * 输入参数：
    *   idxTno       入参, 指定需要加入队列的定时器控制块索引
    * 返 回 值：
    *   RET_OK  成功
    *   其他        失败, 见返回值定义的具体错误
    * 其它说明：从应用看来, 定时器由idxTno和byTimerNo唯一规定, 某个任务的定时器不能由其他任务操作, 
    *           否则会引起不可预料的异常
    **/
    private int addLevelTimerQue(int idxTcb)
    {
        int iRet = OssCoreConstants.RET_ERROR;

        if (idxTcb < 0 || idxTcb >= TMCB_NUM)
        {
            log.error("addLevelTimerQue: param invalid, idxTcb=" + idxTcb);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        TimerControlBlock tcb = lstTCB.get(idxTcb);
        if (IN_USE != tcb.iUse)
        {
            log.error("addLevelTimerQue: TCB[" + idxTcb + "] is not IN_USE, use_status=" + tcb.iUse);
            return OssCoreConstants.RET_ERROR;
        }

        /* 检测时长, 确定该定时器应该加入的队列 */
        if (tcb.iCount < 10 || tcb.iCount >= 10000)
        {
            log.error("addLevelTimerQue: TCB[" + idxTcb + "]'s iCount[" + tcb.iCount + "] invalid");
            return OssCoreConstants.RET_ERROR;
        }

        /* 异常判断, 相对定时器分解队列只能是[1-9] [11-19] [21-29] [31-39] */
        if (0 == (tcb.iQue % 10) || tcb.iQue > 40)
        {
            log.error("addLevelTimerQue: TCB[" + idxTcb + "]'s iQue[" + tcb.iQue + "] invalid");
            return OssCoreConstants.RET_ERROR;
        }
        int iCurQue = tcb.iQue;

        /* 异常判断 */
        TimerQueue tq = lstTq.get(iCurQue);
        if ((0 == tq.iTmcbNum && (INVALID_VALUE != tq.iHead || INVALID_VALUE != tq.iTail)) || (0 != tq.iTmcbNum && (INVALID_VALUE == tq.iHead || INVALID_VALUE == tq.iTail)))
        {
            log.error("addLevelTimerQue: TmQue[" + iCurQue + "] error. TmcbNum=" + tq.iTmcbNum + ", Head=" + tq.iHead + ", Tail=" + tq.iTail);
            return OssCoreConstants.RET_ERROR;
        }

        /* 将定时器控制块加入队列iCurQue, 肯定是加载链表尾 */
        if (0 == tq.iTmcbNum)
        {
            /* 队列为空时, 剩余时间先初始化为0, 后面在计算 */
            tq.iRest = 0;
            tq.iHead = idxTcb;
            tq.iTail = idxTcb;
            tcb.iNext = INVALID_VALUE;
            tcb.iPrev = INVALID_VALUE;
        }
        else
        {
            /* 异常判断 */
            if (tq.iTail > TMCB_NUM)
            {
                log.error("addLevelTimerQue: ptTmQue->wTail(" + tq.iTail + ") > TMCB_NUM");
                return OssCoreConstants.RET_ERROR;
            }

            TimerControlBlock tcbTail = lstTCB.get(tq.iTail);
            if (IN_USE != tcbTail.iUse)
            {
                log.error("addLevelTimerQue: tail_tcb[" + tq.iTail + "]'s status[" + tcbTail.iUse + "] error, not IN_USE");
                return OssCoreConstants.RET_ERROR;
            }

            tcbTail.iNext = idxTcb;
            tcb.iNext = INVALID_VALUE;
            tcb.iPrev = tq.iTail;
            tq.iTail = idxTcb;
        }

        tq.iTmcbNum++;
        tcb.iRest = tq.iCount - tq.iRest;
        tq.iRest = tq.iCount;

        return OssCoreConstants.RET_OK;
    }

    /**
    * 函数名称：Add0Or40TimerQue
    * 功能描述：将TMCB加入队列0或40, 0队列是<1秒的定时器, 40队列是>=1000秒的定时器
    * 输入参数：
    *   wTmcb       入参, 指定需要加入队列的定时器控制块索引
    * 返 回 值：
    *   RET_OK  成功
    *   其他          失败, 见返回值定义的具体错误
    * 其它说明：从应用看来, 定时器由wTno和byTimerNo唯一规定, 某个任务的定时器不能由其他任务操作, 
    *           否则会引起不可预料的异常
    **/
    private int add0Or40TimerQue(int idxTcb)
    {
        int iRet = OssCoreConstants.RET_ERROR;
        int iQueue = INVALID_VALUE;

        if (idxTcb < 0 || idxTcb >= TMCB_NUM)
        {
            log.error("add0Or40TimerQue: param invalid, idxTcb=" + idxTcb);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        TimerControlBlock tcb = lstTCB.get(idxTcb);
        if (IN_USE != tcb.iUse)
        {
            log.error("add0Or40TimerQue: TCB[" + idxTcb + "] is not IN_USE, use_status=" + tcb.iUse);
            return OssCoreConstants.RET_ERROR;
        }

        /* 检测时长, 确定该定时器应该加入的队列 */
        if (tcb.iCount < 10)
        {
            iQueue = 0;
        }
        else if (tcb.iCount >= 10000)
        {
            iQueue = 40;
        }
        else
        {
            log.error("add0Or40TimerQue: tcb[" + idxTcb + "]'s iCount=" + tcb.iCount + " invalid");
            return OssCoreConstants.RET_ERROR;
        }

        TimerQueue tq = lstTq.get(iQueue);
        tcb.iQue = iQueue;
        tcb.iQue1s = INVALID_VALUE;
        tcb.iQue10s = INVALID_VALUE;
        tcb.iQue100s = INVALID_VALUE;

        /* 异常判断 */
        if ((0 == tq.iTmcbNum && (INVALID_VALUE != tq.iHead || INVALID_VALUE != tq.iTail)) || (0 != tq.iTmcbNum && (INVALID_VALUE == tq.iHead || INVALID_VALUE == tq.iTail)))
        {
            log.error("add0Or40TimerQue: TmQue[" + iQueue + "] error. count=" + tq.iTmcbNum + ", head=" + tq.iHead + ", tail=" + tq.iTail);

            return OssCoreConstants.RET_ERROR;
        }

        TimerControlBlock tmpTcbPrev = null;
        TimerControlBlock tmpTcb = null;
        int idxTmpPrevTcb = INVALID_VALUE;
        int idxTmpTcb = tq.iHead;
        int iAdd100ms = 0;
        while (INVALID_VALUE != idxTmpTcb)
        {
            if (idxTmpTcb < 0 || idxTmpTcb >= TMCB_NUM)
            {
                log.error("add0Or40TimerQue: TmQue[" + iQueue + "] error.  iTmpTcb(" + idxTmpTcb + ") invalid");
                return OssCoreConstants.RET_ERROR;
            }
            tmpTcb = lstTCB.get(idxTmpTcb);

            if (IN_USE != tmpTcb.iUse)
            {
                log.error("add0Or40TimerQue: TmQue[" + iQueue + "]  INUSE != tcb[" + idxTmpTcb + "].iUse(" + tmpTcb.iUse + ")");
                return OssCoreConstants.RET_ERROR;
            }

            iAdd100ms += tmpTcb.iRest;
            if (iAdd100ms > tcb.iCount)
            {
                /* 找到插入位置了, 放在tmpTcb之前 */
                break;
            }

            idxTmpPrevTcb = idxTmpTcb;
            tmpTcbPrev = tmpTcb;
            idxTmpTcb = tmpTcb.iNext;
        }

        /* 根据idxTmpPrevTcb、idxTmpTcb判断链表情况 
         * idxTmpPrevTcb、idxTmpTcb都是INVALID_TMQUE_INDEX:    则链表为空
         * idxTmpPrevTcb是INVALID_TMQUE_INDEX, idxTmpTcb不是:  则作为首节点
         * idxTmpPrevTcb不是, idxTmpTcb是INVALID_TMQUE_INDEX:  则作为尾节点
         * idxTmpPrevTcb、idxTmpTcb都不是INVALID_TMQUE_INDEX:  则作为中间节点插入
         */
        if (INVALID_VALUE == idxTmpTcb && INVALID_VALUE == idxTmpPrevTcb)
        {
            /* 空链表 */
            tcb.iRest = tcb.iCount;
            tcb.iPrev = INVALID_VALUE;
            tcb.iNext = INVALID_VALUE;

            tq.iTmcbNum++;
            tq.iCount = tcb.iCount;
            tq.iRest = tcb.iCount;
            tq.iHead = idxTcb;
            tq.iTail = idxTcb;
        }
        else if (INVALID_VALUE == idxTmpPrevTcb)
        {
            /* 首节点 */
            tcb.iRest = tcb.iCount;
            tcb.iPrev = INVALID_VALUE;
            tcb.iNext = idxTmpTcb;

            tmpTcb.iRest = tmpTcb.iRest - tcb.iRest;
            tmpTcb.iPrev = idxTcb;

            tq.iTmcbNum++;
            tq.iHead = idxTcb;
        }
        else if (INVALID_VALUE == idxTmpTcb)
        {
            /* 尾节点 */
            tcb.iRest = tcb.iCount - iAdd100ms;
            tcb.iPrev = idxTmpPrevTcb;
            tcb.iNext = INVALID_VALUE;

            tmpTcbPrev.iNext = idxTcb;

            tq.iTmcbNum++;
            tq.iCount = tcb.iCount;
            tq.iRest = tcb.iCount;
            tq.iTail = idxTcb;
        }
        else
        {
            /* 中间节点 */
            tcb.iRest = tcb.iCount - (iAdd100ms - tmpTcb.iRest);
            tcb.iPrev = idxTmpPrevTcb;
            tcb.iNext = idxTmpTcb;

            tmpTcb.iRest = tmpTcb.iRest - tcb.iRest;
            tmpTcb.iPrev = idxTcb;
            tmpTcbPrev.iNext = idxTcb;

            tq.iTmcbNum++;
        }

        return OssCoreConstants.RET_OK;
    }

    /**
    * 函数名称：addRelTimerQue
    * 功能描述：将相对定时器加入队列中
    * 输入参数：
    *   idxTcb       入参, 指定需要加入队列的定时器控制块索引
    * 返 回 值：
    *   RET_OK  成功
    *   其他        失败, 见返回值定义的具体错误
    * 其它说明：相对定时器的队列分为分级队列、0/40队列两种
    **/
    private int addRelTimerQue(int idxTcb)
    {
        int iRet = OssCoreConstants.RET_ERROR;

        if (idxTcb < 0 || idxTcb >= TMCB_NUM)
        {
            log.error("addRelTimerQue: param invalid, idxTcb=" + idxTcb);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        TimerControlBlock tcb = lstTCB.get(idxTcb);
        if (IN_USE != tcb.iUse)
        {
            log.error("addRelTimerQue: TCB[" + idxTcb + "] is not IN_USE, use_status=" + tcb.iUse);
            return OssCoreConstants.RET_ERROR;
        }

        /* 检测时长, 确定该定时器应该加入的队列; 相对定时器, 时间单位是百毫秒 */
        if (tcb.iCount >= 10 && tcb.iCount < 10000)
        {
            int tmpQue = INVALID_VALUE;

            /* [10, 10000)定时器, 进行队列分解; 100ms->1s->10s->100s队列周期越大, 越迟触发 */
            int iLevelQue = (tcb.iCount % 10000) / 1000;
            if (iLevelQue > 0)
            {
                /* 100s秒队列最后触发, 下面的小周期队列会覆盖ptTmcb->byQue, 依次类推 */
                tcb.iQue100s = iLevelQue + 30;
                tmpQue = tcb.iQue100s;
            }

            iLevelQue = (tcb.iCount % 1000) / 100;
            if (iLevelQue > 0)
            {
                tcb.iQue10s = iLevelQue + 20;
                tmpQue = tcb.iQue10s;
            }

            iLevelQue = (tcb.iCount % 100) / 10;
            if (iLevelQue > 0)
            {
                tcb.iQue1s = iLevelQue + 10;
                tmpQue = tcb.iQue1s;
            }

            iLevelQue = (tcb.iCount % 10);
            if (iLevelQue > 0)
            {
                tcb.iQue = iLevelQue;
            }
            else
            {
                /* 如果没有毫秒时间, 则需要保证一个定时器的分级队列在[iQue, iQue1s, iQue10s, iQue100s]中没有重复 */
                tcb.iQue = tmpQue;
                if (tmpQue >= 30)
                {
                    tcb.iQue100s = INVALID_VALUE;
                }
                else if (tmpQue >= 20)
                {
                    tcb.iQue10s = INVALID_VALUE;
                }
                else if (tmpQue >= 10)
                {
                    tcb.iQue1s = INVALID_VALUE;
                }
            }

            if (tcb.iQue < 0)
            {
                log.error("addRelTimerQue: TCB[" + idxTcb + "]'s iQue is invalid");
                return OssCoreConstants.RET_ERROR;
            }

            iRet = addLevelTimerQue(idxTcb);
        }
        else
        {
            /* 其他, 放入0或40队列 */
            iRet = add0Or40TimerQue(idxTcb);
        }

        return OssCoreConstants.RET_OK;
    }

    /**
    * 函数名称：ScanRelTimerQue
    * 功能描述：扫描一个相对定时器队列
    * 输入参数：
    *   iQueue-入参, 指定需要扫描的队列号
    * 返 回 值：无
    * 其它说明：每隔100ms扫描一次具体的定时器队列
    **/
    private void scanRelTimerQue(int iQueue)
    {
        boolean bAdjustTimer = false;
        if (iQueue < 0 || iQueue > 41)
        {
            log.error("scanRelTimerQue: invalid value, iQueue=" + iQueue);
            return;
        }

        /* 异常判断 */
        TimerQueue tq = lstTq.get(iQueue);
        if ((0 == tq.iTmcbNum && (INVALID_VALUE != tq.iHead || INVALID_VALUE != tq.iTail || tq.iRest > 0)) || (0 != tq.iTmcbNum && (INVALID_VALUE == tq.iHead || INVALID_VALUE == tq.iTail || tq.iRest <= 0)))
        {
            log.error("scanRelTimerQue: invalid value, iQueue=" + iQueue + ", iTmcbNum=" + tq.iTmcbNum + ", iHead=" + tq.iHead + ", iTail=" + tq.iTail + ", iRest=" + tq.iRest);
            return;
        }

        /* 队列为空则不用处理 */
        if (0 == tq.iTmcbNum)
        {
            return;
        }
        tq.iRest--;

        int idxTmpTcb = tq.iHead;
        if (idxTmpTcb < 0 || idxTmpTcb >= TMCB_NUM)
        {
            log.error("scanRelTimerQue: invalid value, idxTmpTcb=" + idxTmpTcb);
            return;
        }

        TimerControlBlock tmpTcb = lstTCB.get(idxTmpTcb);
        if (tmpTcb.iRest > 0)
        {
            tmpTcb.iRest--;
        }
        else
        {
            /* 触发时间为0的, 要么不会被设置, 要么前一个100ms就被触发的, 记录异常日志, 但是不返回错误 */
            log.error("scanRelTimerQue: invalid value, iQueue=" + iQueue + ", idxTmpTcb=" + idxTmpTcb + ", iRest=" + tmpTcb.iRest);
        }

        /* 级差队列, 首节点未到期, 后面的节点肯定不会到期 */
        if (tmpTcb.iRest > 0)
        {
            return;
        }

        /* 当前定时器到期(0==uRest)需要触发, 后面同时到期的定时器同样触发, 所以遍历 */
        while (INVALID_VALUE != idxTmpTcb)
        {
            /* 由于是级差链表中, 一个TMCB的剩余时间不为0, 则后面的定时器都不会在本次触发 */
            tmpTcb = lstTCB.get(idxTmpTcb);
            if (tmpTcb.iRest > 0)
            {
                break;
            }

            /* 触发定时器  */
            if ((0 == iQueue) || (40 == iQueue) || (INVALID_VALUE == tmpTcb.iQue1s && INVALID_VALUE == tmpTcb.iQue10s && INVALID_VALUE == tmpTcb.iQue100s))
            {
                /* 创建定时器消息 */
                String srcTaskName = OssMgrModule.getInstance().getTask(tmpTcb.iTno).getTaskName();
                int timerMsgId = OssCoreConstants.MSG_ID_TIMER_MSG_START + tmpTcb.iTimerNo;
                int iRet = sendMsg(timerMsgId, null, tmpTcb.iTno);
                if (OssCoreConstants.RET_OSS_MSG_QUEUE_FULL == iRet)
                {
                    /** 异常处理. 定时器消息不能丢, 推迟一个时间粒度, 依然放在队列中
                    tmpTcb.iRest++;
                    tq.iRest++;
                    **/

                    log.error("scanRelTimerQue send timer" + tmpTcb.iTimerNo + " to task[" + srcTaskName + ":" + tmpTcb.iTno + "], but msg queue full.");
                    break;
                }
                else if (OssCoreConstants.RET_OK != iRet)
                {
                    log.error("scanRelTimerQue send timer" + tmpTcb.iTimerNo + " to task[" + srcTaskName + ":" + tmpTcb.iTno + "] fail, ret=" + iRet);
                    break;
                }
                else
                {
                    /* 调试打印日志 */
                    // log.debug("scanRelTimerQue send timer" + tmpTcb.iTimerNo
                    // + " to task[" + srcTaskName + ":" + tmpTcb.iTno + "]
                    // success");
                }

                /* 不需要调整队列, 后面直接删除 */
                bAdjustTimer = false;
            }
            else
            {
                /* 将分级定时器移至更高级别的有效队列 */
                if (INVALID_VALUE != tmpTcb.iQue1s)
                {
                    tmpTcb.iQue = tmpTcb.iQue1s;
                    tmpTcb.iQue1s = INVALID_VALUE;
                }
                else if (INVALID_VALUE != tmpTcb.iQue10s)
                {
                    tmpTcb.iQue = tmpTcb.iQue10s;
                    tmpTcb.iQue10s = INVALID_VALUE;
                }
                else if (INVALID_VALUE != tmpTcb.iQue100s)
                {
                    tmpTcb.iQue = tmpTcb.iQue100s;
                    tmpTcb.iQue100s = INVALID_VALUE;
                }
                else
                {
                    /* 异常判断 */
                    log.error("RelTimerProc: TmQue[" + iQueue + "] adjust error, tno=" + tmpTcb.iTno + ", timerno=" + tmpTcb.iTimerNo);
                    break;
                }

                /* 要调整队列, 后面不能删除 */
                bAdjustTimer = true;
            }

            /* 异常判断 */
            int idxTmpNextTcb = tmpTcb.iNext;
            if ((1 == tq.iTmcbNum && INVALID_VALUE != idxTmpNextTcb) || (1 != tq.iTmcbNum && INVALID_VALUE == idxTmpNextTcb))
            {
                log.error("RelTimerProc: TmQue[" + iQueue + "] error, TmQue's tno_num=" + tq.iTmcbNum + ", timerno=" + tmpTcb.iTimerNo);
            }

            /* 将定时器从当前队列移除 */
            if (INVALID_VALUE == idxTmpNextTcb)
            {
                /* 队列中只剩下这一个节点 */
                tq.iTmcbNum = 0;
                tq.iHead = INVALID_VALUE;
                tq.iTail = INVALID_VALUE;
                tq.iRest = 0;
            }
            else
            {
                if (idxTmpNextTcb >= TMCB_NUM)
                {
                    log.error("scanRelTimerQue: TmQue[" + iQueue + "] error. idxTmpNextTcb=" + idxTmpNextTcb + " invalid");
                    break;
                }

                TimerControlBlock tmpNextTcb = lstTCB.get(idxTmpNextTcb);
                if (IN_USE != tmpNextTcb.iUse || iQueue != tmpNextTcb.iQue)
                {
                    log.error("scanRelTimerQue: TmQue[" + iQueue + "] error. tcb[" + idxTmpNextTcb + "]'s info error");
                    break;
                }

                tq.iTmcbNum--;
                tmpNextTcb.iPrev = INVALID_VALUE;
                tq.iHead = idxTmpNextTcb;
            }

            if (bAdjustTimer)
            {
                /* 需要调整, 加入新的分解队列 */
                int iRet = addLevelTimerQue(idxTmpTcb);
                if (OssCoreConstants.RET_OK != iRet)
                {
                    log.error("scanRelTimerQue: addRelTimerQue(tcb=" + idxTmpTcb + ") fail, return " + iRet);
                    break;
                }
            }
            else
            {
                /* 循环定时器需要重置 */
                if (TIMER_TYPE_LOOP == tmpTcb.iType)
                {
                    tmpTcb.iRest = 0;
                    tmpTcb.iQue = INVALID_VALUE;
                    tmpTcb.iQue1s = INVALID_VALUE;
                    tmpTcb.iQue10s = INVALID_VALUE;
                    tmpTcb.iQue100s = INVALID_VALUE;
                    tmpTcb.iPrev = INVALID_VALUE;
                    tmpTcb.iNext = INVALID_VALUE;

                    int iRet = addRelTimerQue(idxTmpTcb);
                    if (OssCoreConstants.RET_OK != iRet)
                    {
                        log.error("scanRelTimerQue: addRelTimerQue idxTmpTcb[" + idxTmpTcb + "] fail, return " + iRet);
                        break;
                    }
                }
                else
                {
                    /* 非循环定时器, 直接删除, 将定时器块释放到空闲队列中 */
                    releaseTcb(idxTmpTcb);
                }
            }

            /* 当前首节点处理完, 继续处理下一个节点 */
            idxTmpTcb = idxTmpNextTcb;
        }
    }

    /**
    * 函数名称：RelTimerProc
    * 功能描述：处理相对定时器, 包括循环定时器
    * 输入参数：无
    * 返 回 值：   无
    */
    private void relTimerProc()
    {
        synchronized (ossTimerTask)
        {
            /* 0: 用于存放定时时间在(0~1)秒之间的定时器 */
            scanRelTimerQue(0);

            /* 对于分级的相对定时器, 需要先扫描长周期队列; 
             * 因为短周期队列中的定时器被调整到长周期队列中, 会在本次扫描中多减掉100ms 
             */
            int iQueue;
            for (iQueue = 39; iQueue > 30; iQueue--)
            {
                scanRelTimerQue(iQueue);
            }

            for (iQueue = 29; iQueue > 20; iQueue--)
            {
                scanRelTimerQue(iQueue);
            }

            for (iQueue = 19; iQueue > 10; iQueue--)
            {
                scanRelTimerQue(iQueue);
            }

            for (iQueue = 9; iQueue > 0; iQueue--)
            {
                scanRelTimerQue(iQueue);
            }

            /* 40: 用于存放 [1000s, +) 的定时器 */
            scanRelTimerQue(40);
        }
    }

    private int getTcb()
    {
        /* 异常判断 */
        if (tcbPool.iIdleNum > TMCB_NUM || tcbPool.iHead > TMCB_NUM || tcbPool.iTail > TMCB_NUM)
        {
            log.error("getTcb: error. iIdleNum=" + tcbPool.iIdleNum + ",iHead=" + tcbPool.iHead + ",iTail=" + tcbPool.iTail);
            return OssCoreConstants.RET_ERROR;
        }

        if ((0 == tcbPool.iIdleNum && tcbPool.iHead != tcbPool.iTail) || (0 != tcbPool.iIdleNum && tcbPool.iHead == tcbPool.iTail))
        {
            log.error("getTcb: error. iIdleNum=" + tcbPool.iIdleNum + ",iHead=" + tcbPool.iHead + ",iTail=" + tcbPool.iTail);
            return OssCoreConstants.RET_ERROR;
        }

        /* 判断空闲链表是否为空 */
        if (0 == tcbPool.iIdleNum)
        {
            return OssCoreConstants.RET_ERROR;
        }

        int idxTcb = tcbPool.aiIdleList[tcbPool.iHead];
        tcbPool.iHead = (tcbPool.iHead + 1) % (TMCB_NUM + 1);
        tcbPool.iIdleNum--;
        if (tcbPool.iMaxUsed < TMCB_NUM - tcbPool.iIdleNum)
        {
            tcbPool.iMaxUsed = TMCB_NUM - tcbPool.iIdleNum;
        }

        /* 对wTmcb进行异常判断*/
        if (idxTcb >= TMCB_NUM)
        {
            log.error("getTcb: error. idxTcb=" + idxTcb + " invalid");
            return OssCoreConstants.RET_ERROR;
        }

        if (UN_USE != lstTCB.get(idxTcb).iUse)
        {
            log.error("getTcb: error. tcb[" + idxTcb + "]'s value=" + lstTCB.get(idxTcb).iUse + " invalid");
            return OssCoreConstants.RET_ERROR;
        }

        /* 获取该TMCB */
        lstTCB.get(idxTcb).iUse = IN_USE;

        return idxTcb;
    }

    private int releaseTcb(int idxTcb)
    {
        if (idxTcb < 0 || idxTcb >= TMCB_NUM)
        {
            log.error("releaseTcb: param invalid, idxTcb=" + idxTcb);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        /* 异常判断 */
        if (IN_USE != lstTCB.get(idxTcb).iUse)
        {
            log.error("releaseTcb: tcb[" + idxTcb + "]'s use_status invalid, use_status=" + lstTCB.get(idxTcb).iUse);
            return OssCoreConstants.RET_ERROR;
        }

        if (tcbPool.iIdleNum > TMCB_NUM || tcbPool.iHead > TMCB_NUM || tcbPool.iTail > TMCB_NUM)
        {
            log.error("releaseTcb: tcbPool info error, iIdleNum=" + tcbPool.iIdleNum + ", iHead=" + tcbPool.iHead + ", iTail=" + tcbPool.iTail);
            return OssCoreConstants.RET_ERROR;
        }

        if ((TMCB_NUM == tcbPool.iIdleNum && tcbPool.iHead != (tcbPool.iTail + 1) % (TMCB_NUM + 1)) || (TMCB_NUM != tcbPool.iIdleNum && tcbPool.iHead == (tcbPool.iTail + 1) % (TMCB_NUM + 1)))
        {
            log.error("releaseTcb: tcbPool info error, iIdleNum=" + tcbPool.iIdleNum + ", iHead=" + tcbPool.iHead + ", iTail=" + tcbPool.iTail);
            return OssCoreConstants.RET_ERROR;
        }

        /* 判断空闲链表是否为满 */
        if (TMCB_NUM == tcbPool.iIdleNum)
        {
            return OssCoreConstants.RET_OSS_TCB_IDLE_FULL;
        }

        /* 回收 */
        lstTCB.get(idxTcb).iUse = UN_USE;
        tcbPool.aiIdleList[tcbPool.iTail] = idxTcb;
        tcbPool.iTail = (tcbPool.iTail + 1) % (TMCB_NUM + 1);
        tcbPool.iIdleNum++;

        // log.debug("releaseTcb tcb[" + idxTcb + "]");
        return OssCoreConstants.RET_OK;
    }

    /**
    * 函数名称：setTimer
    * 功能描述：模块对外接口, 用于定时器设置
    * 输入参数：
    *   iTimerNo   入参, 定时器在目的任务中的编号
    *   iParam     入参, 默认参数参数
    * 返 回 值：
    *   RET_OK  成功
    *   其他        失败, 见返回值定义的具体错误
    * 其它说明：从应看来, 定时器由wTno和byTimerNo唯一规定, 某个任务的定时器不能由其他任务操作,
    *           调用本接口时, 只能给本线程对应的任务设置定时器. 
    *           所以, 这几个定时器操作接口没有同步问题
    **/
    public static int setTimer(int srcTno, int timerType, int iTimerNo, int iCount, int iParam)
    {
        /* 调试打印日志 */
        log.trace("ossSetTimer tno=" + srcTno + ", timerType=" + timerType + ", iTimerNo=" + iTimerNo + ", iCount=" + iCount + ", iParam" + iParam);
        if (null == ossTimerTask)
        {
            log.error("setTimer: null == ossTimerTask");
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        if (iTimerNo < OssCoreConstants.MIN_TIMER_NO || iTimerNo > OssCoreConstants.MAX_TIMER_NO || 0 == iCount)
        {
            log.error("setTimer: param error. iTimerNo=" + iTimerNo + ", iCount=" + iCount);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        if (srcTno < OssCoreConstants.TNO_START || srcTno > OssCoreConstants.TNO_OVER)
        {
            log.error("setTimer: param error. srcTno=" + srcTno);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        if (TIMER_TYPE_LOOP != timerType && TIMER_TYPE_ABS != timerType && TIMER_TYPE_REL != timerType)
        {
            log.error("setTimer: param error. timerType=" + timerType);
            return OssCoreConstants.RET_OSS_PARAM_ERROR;
        }

        synchronized (ossTimerTask)
        {
            int idxTcb = getInstance().getTcb();
            if (OssCoreConstants.RET_ERROR == idxTcb)
            {
                return idxTcb;
            }
            TimerControlBlock tcb = getInstance().lstTCB.get(idxTcb);

            /* 初始化TMCB, 一旦获取, 只能给同一(任务)线程使用 */
            tcb.iType = timerType;
            tcb.iTimerNo = iTimerNo;
            tcb.iTno = srcTno;
            tcb.iCount = iCount;
            tcb.iRest = 0;
            tcb.iQue = INVALID_VALUE;
            tcb.iQue1s = INVALID_VALUE;
            tcb.iQue10s = INVALID_VALUE;
            tcb.iQue100s = INVALID_VALUE;
            tcb.iPrev = INVALID_VALUE;
            tcb.iNext = INVALID_VALUE;

            /* 按照定时器类型, 加入相应队列 */
            int iRet = OssCoreConstants.RET_ERROR;
            if (TIMER_TYPE_LOOP == timerType || TIMER_TYPE_REL == timerType)
            {
                iRet = getInstance().addRelTimerQue(idxTcb);
            }
            else
            {
                getInstance().releaseTcb(idxTcb);

                log.error("setTimer: timerType=" + timerType + " invalid");

                return OssCoreConstants.RET_ERROR;
            }

            if (OssCoreConstants.RET_OK != iRet)
            {
                getInstance().releaseTcb(idxTcb);

                log.error("setTimer: addRelTimerQue error, srcTno=" + srcTno + ", timerType=" + timerType + ", iTimerNo=" + iTimerNo + ", iCount=" + iCount);

                return OssCoreConstants.RET_ERROR;
            }
        }

        return OssCoreConstants.RET_OK;
    }
}
