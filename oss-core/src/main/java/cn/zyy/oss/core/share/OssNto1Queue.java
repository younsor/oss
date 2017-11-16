package cn.zyy.oss.core.share;

import java.util.concurrent.locks.ReentrantLock;

import cn.zyy.oss.share.OssLog;

public class OssNto1Queue
{
    private static final OssLog log             = new OssLog();
    public static final int     RET_OK          = 0;
    public static final int     RET_ERROR       = 1;
    public static final int     RET_QUEUE_FULL  = 2;
    public static final int     RET_QUEUE_EMPTY = 3;

    private ReentrantLock       lock            = null;
    private final int           maxSize;
    private Object[]            arrayBuff       = null;
    private int                 writerPos;                     /* 当前可写位置, 该位置无数据 */
    private int                 readerPos;                     /* 当前可读位置, 该位置有数据 */

    public OssNto1Queue(int size)
    {
        this.maxSize = size;
        arrayBuff = new Object[maxSize + 1];
        lock = new ReentrantLock();
        writerPos = 0;
        readerPos = 0;
    }

    private int nextPos(int pos)
    {
        return ((pos + 1) % (maxSize + 1));
    }

    public boolean isEmpty()
    {
        return (writerPos == readerPos);
    }

    public boolean isFull()
    {
        return (readerPos == nextPos(writerPos));
    }

    public int size()
    {
        int tmpWriterPos = writerPos;
        int tmpReaderPos = readerPos;
        return (tmpWriterPos - tmpReaderPos + maxSize + 1) % (maxSize + 1);
    }

    public int add(Object data)
    {
        /* 预判是否为满
         * 以一种大概率的判断满的方式，尽可能避免锁操作 */
        if (readerPos == writerPos + 1)
        {
            return RET_QUEUE_FULL;
        }

        /* 真正的增加数据 */
        lock.lock();
        try
        {
            int tmpNextWriterPos = nextPos(writerPos);
            if (readerPos == tmpNextWriterPos)
            {
                /* 真正的满, 还是返回 */
                return RET_QUEUE_FULL;
            }

            /* 不满则写入 */
            arrayBuff[tmpNextWriterPos] = data;
            writerPos = tmpNextWriterPos;
        }
        finally
        {
            lock.unlock();
        }

        return RET_OK;
    }

    public int addNoLock(Object data)
    {
        /* 真正的增加数据 */
        int tmpNextWriterPos = ((writerPos + 1) % (maxSize + 1));
        if (readerPos == tmpNextWriterPos)
        {
            /* 满 */
            return RET_QUEUE_FULL;
        }

        /* 不满则写入 */
        arrayBuff[tmpNextWriterPos] = data;
        writerPos = tmpNextWriterPos;

        return RET_OK;
    }

    public Object get()
    {
        if (readerPos == writerPos)
        {
            return null;
        }

        Object data = arrayBuff[readerPos];

        /* 移到下一个可读位置 */
        readerPos = nextPos(readerPos);

        return data;
    }
}
