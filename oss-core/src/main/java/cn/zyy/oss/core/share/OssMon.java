package cn.zyy.oss.core.share;

import java.util.List;

import com.google.common.collect.Lists;

import cn.zyy.oss.core.module.OssMonModule;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssMon
{
    private static final OssLog log                   = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int    MIN_MON_INFO_NODE_NUM = 20;

    public static abstract class MonValue
    {
        public abstract MonValue merge(MonValue thatValue) throws Exception;

        public abstract void clear();

        public abstract String toString();
    };

    public static class AveValue extends MonValue
    {
        private long value = 0;
        private long num   = 0;

        public long num()
        {
            return num;
        }

        public long value()
        {
            return value;
        }

        public void add(long newValue)
        {
            num++;
            value += newValue;
        }

        @Override
        public void clear()
        {
            value = 0;
            num = 0;
        }

        @Override
        public MonValue merge(MonValue thatValue) throws Exception
        {
            if (!(thatValue instanceof AveValue))
            {
                throw new Exception("thatValue not instanceof AveValue");
            }

            AveValue thatAveValue = (AveValue) thatValue;
            this.num += thatAveValue.num;
            this.value += thatAveValue.value;

            return this;
        }

        @Override
        public String toString()
        {
            return "num=" + num + "-value=" + value;
        }
    };

    public static class MonNode
    {
        /**secondTime 表示当前Node存储的是哪个秒时间的信息
         * 由应用层写, 支撑层读取, 主要用于校验
         * 当应用层没有写信息时, 则为0*/
        private long           secondTime = 0;
        private List<AveValue> lstValue   = Lists.newArrayList();

        private MonNode(int num)
        {
            for (int idx = 0; idx < num; idx++)
            {
                lstValue.add(new AveValue());
            }
        }

        private long getSecondTime()
        {
            return secondTime;
        }

        private void setSecondTime(long second)
        {
            secondTime = second;
        }

        private void add(int idx, long value) throws Exception
        {
            if (idx >= lstValue.size())
            {
                throw new Exception("idx >= lstValue's size(" + lstValue.size() + ")");
            }
            else
            {
                lstValue.get(idx).add(value);
            }
        }

        private void merge(MonNode node)
        {
            if (lstValue.size() != node.lstValue.size())
            {
                log.error("lstValue.size(%s) != node.lstValue.size(%s)", lstValue.size(), node.lstValue.size());
                return;
            }

            for (int idx = 0; idx < lstValue.size(); idx++)
            {
                try
                {
                    lstValue.get(idx).merge(node.lstValue.get(idx));
                }
                catch (Exception e)
                {
                    log.error("lstValue.get(%s).merge exception, info as follow: \n%s" + OssFunc.getExceptionInfo(e));
                    return;
                }
            }
        }

        private void clear()
        {
            secondTime = 0;
            for (AveValue aveValue : lstValue)
            {
                aveValue.clear();
            }
        }

        public List<AveValue> getAllValue()
        {
            return lstValue;
        }

        public int getStatNum()
        {
            return lstValue.size();
        }

        public String toString()
        {
            return lstValue.toString();
        }
    };

    public static class MonInfo
    {
        public int            id;
        public String         type;
        public String         taskName;
        private List<MonNode> lstNode = Lists.newArrayList();
        private final int     nodeNum;

        /**num: 表示性能链表长度, 最小长度为20
         * statNum: 表示单个链表节点中, 性能统计点的个数, 如有4个统计点: A、B、C、D, 那么
         *          MonNode.lstValue[0]表示总延迟
         *          MonNode.lstValue[1]表示A-B之间的延迟
         *          MonNode.lstValue[2]表示B-C之间的延迟
         *          MonNode.lstValue[3]表示C-D之间的延迟
         * */
        public MonInfo(String type, int num, int statNum, long startSecond)
        {
            this.type = type;

            if (num < 20)
            {
                num = 20;
            }

            this.nodeNum = num;
            for (int idx = 0; idx < nodeNum; idx++)
            {
                lstNode.add(new MonNode(statNum));
            }
        }

        public MonNode merge2OtherNodeAndClear(long readSecond, MonNode otherNode)
        {
            long startSecond = OssMonModule.getSysMonStartSecond();
            if (readSecond < startSecond)
            {
                log.error("readSecond(%s) < startSecond(%s) when id=%s, type=%s, taskName=", readSecond, startSecond, id, type, taskName);
                return null;
            }

            int diffPos = (int) (readSecond - startSecond);
            int idxPos = diffPos % nodeNum;
            MonNode tmpMonNode = lstNode.get(idxPos);
            if (0 == tmpMonNode.getSecondTime())
            {
                /* 表示当前没有监控消息 */
                // log.debug("collect %s's monitor-info null from %s",
                // readSecond, taskName);
                return null;
            }
            else if (tmpMonNode.getSecondTime() != readSecond)
            {
                log.error("monnode[%s]'s secondTime(%s) != readSecond(%s) when id=%s, type=%s, taskName=", idxPos, tmpMonNode.getSecondTime(), readSecond, id, type, taskName);
                return null;
            }

            if (null == otherNode)
            {
                otherNode = new MonNode(tmpMonNode.getStatNum());
            }

            otherNode.merge(tmpMonNode);
            log.debug("collect %s's monitor-info from %s, info: %s", readSecond, taskName, tmpMonNode);

            /* 清空, 很重要 */
            tmpMonNode.clear();
            return otherNode;
        }

        public void add(long secTime, List<Long> lstSample) throws Exception
        {
            if (null == lstSample)
            {
                throw new Exception("null == lstSample");
            }

            /* 定位写的位置, 需要遵守如下原则: 
             * 1) 写时间不能在已读时间之前, 因为支撑系统已经延迟读
             * 2) 写时间不能超过读时间一轮, 我们的时间环长度足够长了, 如果超过一轮肯定是有bug */
            long startSecond = OssMonModule.getSysMonStartSecond();
            long diffPos = secTime - startSecond;
            if (diffPos < 0)
            {
                log.error("writeSecTime(%s) <= startSecond(%s), when type=%s, appTaskNo=%s", secTime, startSecond, type, taskName);
                return;
            }

            /* 异常判断, 应用层写信息的时间, 不应该在支撑层收集监控的时间之前 */
            long handlerSecond = OssMonModule.getSysMonCollectSecond();
            long diffSec = secTime - handlerSecond;
            if (diffSec >= MIN_MON_INFO_NODE_NUM - 2)
            {
                log.error("writeSecTime(%s) >= handlerSecond(%s) + nodeNum(%s)-2, when type=%s, appTaskNo=%s", secTime, handlerSecond, nodeNum, type, taskName);
                return;
            }

            int writeIdx = (int) ((diffPos) % nodeNum);
            MonNode writerNode = lstNode.get(writeIdx);
            if (writerNode.secondTime == 0)
            {
                writerNode.secondTime = secTime;
            }
            else
            {
                /* 异常判断 */
                if (writerNode.secondTime != secTime)
                {
                    log.error("monnode[%s]'s secondTime(%s) != writeSecond(%s)", writeIdx, writerNode.secondTime, secTime);
                    return;
                }
            }

            /* 异常判断 */
            if (lstSample.size() != writerNode.lstValue.size())
            {
                throw new Exception("lstSample's size(" + lstSample.size() + ") invalid, != writerNode.lstValue.size(" + writerNode.lstValue.size() + ") + 1");
            }

            long totalDiffValue = 0;
            for (int idx = 0; idx < lstSample.size() - 1; idx++)
            {
                long diffValue = lstSample.get(idx + 1) - lstSample.get(idx);
                if (diffValue < 0)
                {
                    log.error("diffValue(%s) < 0, when idx=%s type=%s appTaskNo=%s", diffValue, idx, type, taskName);
                    diffValue = 0;
                }
                writerNode.add(idx + 1, diffValue);

                totalDiffValue += diffValue;
            }
            writerNode.add(0, totalDiffValue);

            log.debug("record moninfo: second=%s \n\tlstSample=%s \n\twriterNode=%s", secTime, lstSample, writerNode);
        }
    };
}
