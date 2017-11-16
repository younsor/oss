package cn.zyy.oss.core.task;

import cn.zyy.oss.core.share.OssCache;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTask.IOssConsumer;
import cn.zyy.oss.share.OssCacheData;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public abstract class OssTaskCsmCache extends OssTask implements IOssConsumer
{
    private static final OssLog log         = new OssLog(OssLog.LOG_MODULE_OSS);
    protected static final int  MSG_ID_DATA = 1;
    protected static final int  RET_STOP    = 10000;

    private OssCache            cache       = null;
    private String              cacheDir;
    private String              cacheFilePrefix;
    private int                 cacheFileMaxSizeKB;

    /*************************任务自己管理的变量**************************/
    protected OssTaskCsmCache(int taskNo, String taskName, int iPriority)
    {
        super(taskNo, OssCoreConstants.TASK_TYPE_PRODUCER, taskName, iPriority);
    }

    protected void writeData2Log(OssCacheData cacheData)
    {
        log.error("exc-cache-record: " + cacheData.serialString());
    }

    public int startTask(Class<? extends OssCacheData> dataClass, String dir, String filePrefix, int fileMaxSizeKB)
    {
        if (null == dir || null == filePrefix)
        {
            log.error("invalid start-param: cacheDir[" + dir + "] cacheFilePrefix[" + cacheFilePrefix + "]");
            return OssCoreConstants.RET_ERROR;
        }

        cacheDir = dir.trim();
        cacheFilePrefix = filePrefix.trim();
        if (fileMaxSizeKB < 10 * 1024)
        {
            cacheFileMaxSizeKB = 10 * 1024;
        }
        else if (fileMaxSizeKB > 200 * 1024)
        {
            cacheFileMaxSizeKB = 200 * 1024;
        }
        else
        {
            cacheFileMaxSizeKB = fileMaxSizeKB;
        }

        cache = new OssCache(dataClass, cacheDir, cacheFilePrefix, cacheFileMaxSizeKB, 30000);

        /* 准备好之后, 启动任务线程 */
        start();

        return OssCoreConstants.RET_OK;
    }

    protected void taskRun()
    {
        Thread.currentThread().setName(taskName);

        int iRet;
        Object objReadData = null;
        while (true)
        {
            objReadData = cache.get();
            if (null == objReadData)
            {
                /* 如果数据为空, 则等待5ms */
                log.trace("no data in cache, and wait 5ms...");
                OssFunc.sleep(5);
                continue;
            }

            if (!(objReadData instanceof OssCacheData))
            {
                log.error("recv cache data, its class[" + objReadData.getClass().getName() + "] not instanceof OssCache.CacheData");
                continue;
            }

            OssCacheData cacheData = (OssCacheData) objReadData;

            /* 处理数据 */
            try
            {
                iRet = onHandler(MSG_ID_DATA, objReadData);
            }
            catch (Exception e)
            {
                /* 抛异常的数据, 重复处理也没用, 直接写异常日志  */
                log.error("handler msg exception, and write to log\n" + OssFunc.getExceptionInfo(e));
                writeData2Log(cacheData);
                iRet = OssCoreConstants.RET_ERROR;
                continue;
            }

            if (OssConstants.RET_OK == iRet)
            {
                /* 业务层成功处理 */
            }
            else if (RET_STOP == iRet)
            {
                /* 业务层暂停处理, 需要将数据重新存入cache, 并且速度慢下来 */
                iRet = cache.add(cacheData);
                if (OssConstants.RET_OK != iRet)
                {
                    log.error("handler msg ret-stop, put back to buff fail. write to log and wait 500ms");
                    writeData2Log(cacheData);
                }
                else
                {
                    log.error("handler msg ret-stop, put back to buff success. and wait 500ms");
                }

                OssFunc.sleep(500);
            }
            else
            {
                /* 其他错误, 则直接写到日志中 */
                log.error("handler msg error,  and write to log");
                writeData2Log(cacheData);
            }
        }
    }

    protected int add(OssCacheData data)
    {
        return cache.add(data);
    }
}
