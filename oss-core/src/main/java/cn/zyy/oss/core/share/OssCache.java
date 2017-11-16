package cn.zyy.oss.core.share;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;

import cn.zyy.oss.share.OssCacheData;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssCache
{
    private static final OssLog                 log                      = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int                    DEF_RECORD_NUM           = 50000;
    private static final int                    DEF_RECORD_NUM_PER_FLUSH = 20000;
    private static final int                    DEF_RECORD_NUM_PER_READ  = 20000;
    private static final int                    CACHE_FILE_MAX_SIZE_KB   = 51200;
    private static final String                 CACHE_FILE_NAME_POSTFIX  = "yyyyMMdd-HHmmss-SSS";

    public static final int                     RET_ERROR                = -1;
    public static final int                     RET_OK                   = 0;
    public static final int                     RET_NO_DATA              = 1;
    public static final int                     RET_BUFF_FULL            = 2;
    public static final int                     RET_BUFF_EMPTY           = 3;
    public final static int                     RET_NO_CACHE_FILE        = 4;

    private final Class<? extends OssCacheData> recordClass;
    private ReentrantLock                       lock                     = null;
    private OssCacheData[]                      arrayBuff                = null;
    private int                                 writerPos;                                                   /* 当前可写位置, 该位置无数据 */
    private int                                 readerPos;                                                   /* 当前可读位置, 该位置有数据 */

    private String                              cacheDir                 = null;                             /* 缓存数据存储目录 */
    private String                              fileNamePrefix           = null;                             /* 缓存文件名称前缀 */
    private int                                 maxCacheFileSizeKB;                                          /* 缓存文件最大长度, 单位: KB */

    private int                                 maxBuffSize;
    private int                                 perFlashNum;
    private int                                 perReadNum;

    public OssCache(Class<? extends OssCacheData> dataClass, String dir, String name, int maxSizeKB, int initBuffSize)
    {
        this.recordClass = dataClass;

        if (initBuffSize <= 100)
        {
            maxBuffSize = DEF_RECORD_NUM;
            perFlashNum = DEF_RECORD_NUM_PER_FLUSH;
            perReadNum = DEF_RECORD_NUM_PER_READ;
        }
        else
        {
            maxBuffSize = initBuffSize;
            perFlashNum = (int) (((double) DEF_RECORD_NUM_PER_FLUSH / DEF_RECORD_NUM) * maxBuffSize);
            perReadNum = (int) (((double) DEF_RECORD_NUM_PER_READ / DEF_RECORD_NUM) * maxBuffSize);
        }

        log.info("max-buff-bize=" + maxBuffSize + ", per-flash-num=" + perFlashNum + ", per-read-num=" + perReadNum);

        lock = new ReentrantLock();
        arrayBuff = new OssCacheData[maxBuffSize + 1];
        writerPos = 0;
        readerPos = 0;

        cacheDir = dir.trim();
        fileNamePrefix = name;

        if (maxSizeKB <= 1024)
        {
            maxCacheFileSizeKB = CACHE_FILE_MAX_SIZE_KB;
        }
        else
        {
            maxCacheFileSizeKB = maxSizeKB;
        }
    }

    private int nextPos(int pos)
    {
        return ((pos + 1) % (maxBuffSize + 1));
    }

    private boolean isBuffEmpty()
    {
        return (writerPos == readerPos);
    }

    private boolean isBuffFull()
    {
        return (readerPos == nextPos(writerPos));
    }

    private int buffSize()
    {
        int tmpWriterPos = writerPos;
        int tmpReaderPos = readerPos;
        return (tmpWriterPos - tmpReaderPos + maxBuffSize + 1) % (maxBuffSize + 1);
    }

    private int _add(OssCacheData data)
    {
        /* 真正的增加数据 */
        if (isBuffFull())
        {
            /* 满 */
            return RET_BUFF_FULL;
        }

        /* 不满则写入 */
        arrayBuff[writerPos] = data;
        writerPos = nextPos(writerPos);

        return RET_OK;
    }

    private OssCacheData _get()
    {
        if (readerPos == writerPos)
        {
            return null;
        }

        OssCacheData data = arrayBuff[readerPos];

        /* 移到下一个可读位置 */
        readerPos = nextPos(readerPos);
        return data;
    }

    /* 写回退接口, 回退num个数据出来, num为最大回退数
     * 线程不安全接口, 此时接口外围已经lock住
     * writerPos不会变动
     * readerPos会变动 */
    private OssCacheData[] rollback(int num)
    {
        /* 回退的原因是队列满了, 需要回退出一些消息进行文件备份, 因此不需要回退完; 否则会读、写缓存文件, 造成资源浪费
         * 队列中最起码要预留20%长度的数据 */
        int maxRollbackSize = buffSize() - (arrayBuff.length / 10);
        if (maxRollbackSize <= 0)
        {
            /* 异常: 如果当前缓存数据数量 比 缓存长度的10%还要少, 则最大回滚长度就设为当前的数据数量; 理论上应该不会走到这一步 */
            maxRollbackSize = buffSize();
            log.info("rollback error info: buffsize=" + buffSize() + ", arrayBuff.length=" + arrayBuff.length);
        }

        int rollbackNum = (maxRollbackSize > num) ? num : maxRollbackSize;

        /* 需要迁移走的数据是从rollbackWriterPos 到 curWriterPos - 1 */
        int curWriterPos = writerPos;
        int rollbackWriterPos = (writerPos - rollbackNum + maxBuffSize + 1) % (maxBuffSize + 1);

        /* 先重置writerPos */
        writerPos = rollbackWriterPos;

        OssCacheData[] arrayRollbackData = new OssCacheData[rollbackNum];
        for (int idxPos = 0; idxPos < rollbackNum; idxPos++)
        {
            arrayRollbackData[idxPos] = arrayBuff[rollbackWriterPos];
            arrayBuff[rollbackWriterPos] = null;
            rollbackWriterPos = ((rollbackWriterPos + 1) % (maxBuffSize + 1));
        }

        /* 异常判断 */
        if (rollbackWriterPos != curWriterPos)
        {
            log.error("rollbackWriterPos(%s) != curWriterPos(%s)", rollbackWriterPos, curWriterPos);
        }

        return arrayRollbackData;
    }

    private String newCacheFileName()
    {
        Date curDate = new Date();
        String postFix = OssFunc.TimeConvert.Date2Format(curDate, CACHE_FILE_NAME_POSTFIX);
        return fileNamePrefix + postFix;
    }

    private List<String> getTotalContent(String fileName)
    {
        List<String> fileContent = Lists.newArrayList();
        FileReader in = null;
        LineNumberReader reader = null;

        try
        {
            in = new FileReader(fileName);
            reader = new LineNumberReader(in);

            String strLine = reader.readLine();
            while (strLine != null)
            {
                fileContent.add(strLine);
                strLine = reader.readLine();
            }
        }
        catch (Exception e)
        {
            log.error("getTotalContent read file exception...\n" + OssFunc.getExceptionInfo(e));
        }
        finally
        {
            if (null != reader)
            {
                try
                {
                    reader.close();
                }
                catch (IOException e)
                {
                }
            }

            if (null != in)
            {
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        return fileContent;
    }

    private int writeToFile()
    {
        File fCacheDir = new File(cacheDir);
        File[] lstCacheFile = fCacheDir.listFiles();
        boolean isNeedNewFile = false;
        if (null == lstCacheFile || lstCacheFile.length <= 0)
        {
            isNeedNewFile = true;
        }
        else
        {
            /* 此时按照字典排序就对了, 最后一个文件就是当前最新文件 */
            long fileSizeBytes = lstCacheFile[lstCacheFile.length - 1].length();
            int fileSizeKB = (int) (fileSizeBytes / 1024);
            if (fileSizeKB >= maxCacheFileSizeKB)
            {
                /* 如果最有一个文件大小已达限, 则新启动一个文件 */
                isNeedNewFile = true;
            }
        }

        File fWriterFile = null;
        if (isNeedNewFile)
        {
            String cachaFileName = newCacheFileName();
            String cachaFilePath = cacheDir + "/" + cachaFileName;
            fWriterFile = new File(cachaFilePath);
            boolean isCreateOK = false;
            try
            {
                isCreateOK = fWriterFile.createNewFile();
            }
            catch (IOException e)
            {
                isCreateOK = false;
                log.error("create new cache-file(%s) exception. \n%s", fWriterFile.toString(), OssFunc.getExceptionInfo(e));
            }

            if (!isCreateOK)
            {
                return OssConstants.RET_ERROR;
            }
        }
        else
        {
            fWriterFile = lstCacheFile[lstCacheFile.length - 1];
        }

        /* 从共享队列中取一批数据存储到文件中 */
        OssCacheData[] rollbackData = rollback(perFlashNum);
        if (null == rollbackData || rollbackData.length <= 0)
        {
            return OssConstants.RET_ERROR;
        }

        int writerNum = 0;
        FileWriter fw = null;
        BufferedWriter writerBuff = null;
        try
        {
            fw = new FileWriter(fWriterFile, true);
            writerBuff = new BufferedWriter(fw);
            for (int idxPos = 0; idxPos < rollbackData.length; idxPos++)
            {
                String strRecord = rollbackData[idxPos].serialString();

                /* byteRecord末尾自带了换行 */
                writerBuff.write(strRecord);
                writerBuff.newLine();
                writerNum++;
            }

            writerBuff.flush();
            fw.flush();
        }
        catch (Exception e)
        {
            log.error("write cache-file(%s) exception, has writer %s data. \n%s", fWriterFile, writerNum, OssFunc.getExceptionInfo(e));

            /* 异常处理. TODO */
            return OssConstants.RET_ERROR;
        }
        finally
        {
            if (null != writerBuff)
            {
                try
                {
                    writerBuff.close();
                }
                catch (IOException e)
                {
                    log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                }
            }

            if (null != fw)
            {
                try
                {
                    fw.close();
                }
                catch (IOException e)
                {
                    log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                }
            }

            log.info("write %s data to cache-file(%s)", writerNum, fWriterFile);
        }

        return OssConstants.RET_OK;
    }

    private int readFromFile()
    {
        int writerToBuffNum = 0;
        File fCacheDir = new File(cacheDir);
        File[] lstCacheFile = fCacheDir.listFiles();
        if (null == lstCacheFile || lstCacheFile.length <= 0)
        {
            /* 若没有缓存文件, 则返回空 */
            return writerToBuffNum;
        }

        File fReadFile = lstCacheFile[lstCacheFile.length - 1];
        List<String> fileContent = getTotalContent(fReadFile.toString());
        if (null == fileContent || fileContent.size() <= 0)
        {
            /* 文件空为异常情况
             * 文件行数据在被取空的同时就删除了, 不会存在空文件 */
            log.error("readFromFile(%s) fail, because file if empty, and delete it", fReadFile);

            fReadFile.delete();
            return OssConstants.RET_ERROR;
        }

        int readLineNum;
        int initReadLineNo;
        if (fileContent.size() <= perReadNum)
        {
            readLineNum = fileContent.size();
            initReadLineNo = 0;
        }
        else
        {
            readLineNum = perReadNum;
            initReadLineNo = fileContent.size() - perReadNum;
        }

        /* 把从 initReadLineNo 到 fileContent.size()-1 的数据写到共享缓冲区 */
        for (int idx = 0; idx < readLineNum; idx++)
        {
            String strRecord = fileContent.get(initReadLineNo);
            OssCacheData cacheRecord;
            try
            {
                cacheRecord = recordClass.newInstance();
            }
            catch (Exception e)
            {
                log.error("new " + recordClass.getName() + "'s instance exception\n" + OssFunc.getExceptionInfo(e));
                log.error("cache record deserialFromString fail: " + strRecord);
                continue;
            }

            if (null == cacheRecord.deserialFromString(strRecord))
            {
                log.error("cache record deserialFromString fail: " + strRecord);
                continue;
            }

            /** 换行不需要提供 
            byte[] byteSendRecord = new byte[byteRecord.length + byteDelimiter.length];
            System.arraycopy(byteRecord, 0, byteSendRecord, 0, byteRecord.length);
            System.arraycopy(byteDelimiter, 0, byteSendRecord, byteRecord.length, byteDelimiter.length);
            int iRet = _add(byteSendRecord);
            */
            int iRet = _add(cacheRecord);
            if (OssConstants.RET_OK != iRet)
            {
                /* 未加成功, 则退出;  */
                log.error("_add read-log-record to buff, initReadLineNo=%s, readLineNum=%s, idx=%s, iRet=%s", initReadLineNo, readLineNum, idx, iRet);
                break;
            }

            /* 保证已经写入到缓冲区的记录, 从文件中删除 */
            fileContent.remove(initReadLineNo);
            writerToBuffNum++;
        }

        log.info("read %s data from cache-file(%s)...", writerToBuffNum, fReadFile);

        /* 把从 0到initReadLineNo-1 的数据写回文件 */
        if (initReadLineNo > 0 && writerToBuffNum > 0)
        {
            FileWriter fw = null;
            BufferedWriter writerBuff = null;
            try
            {
                fw = new FileWriter(fReadFile);
                writerBuff = new BufferedWriter(fw);
                for (int idxPos = 0; idxPos < fileContent.size(); idxPos++)
                {
                    String strRecord = fileContent.get(idxPos);
                    writerBuff.write(strRecord);
                    writerBuff.newLine();
                }

                writerBuff.flush();
                fw.flush();
            }
            catch (Exception e)
            {
                /* 此时返回错误 */
                log.error("write shengyu-records to read-file(%s) exception. \n%s", fReadFile.toString(), OssFunc.getExceptionInfo(e));

                return writerToBuffNum;
            }
            finally
            {
                if (null != writerBuff)
                {
                    try
                    {
                        writerBuff.close();
                    }
                    catch (IOException e)
                    {
                        log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                    }
                }

                if (null != fw)
                {
                    try
                    {
                        fw.close();
                    }
                    catch (IOException e)
                    {
                        log.error("writerBuff.close exception. \n%s", OssFunc.getExceptionInfo(e));
                    }
                }
            }
        }
        else
        {
            /* 删除该文件 */
            if (!fReadFile.delete())
            {
                if (!fReadFile.delete())
                {
                    log.error("delete read-file(%s) fail.", fReadFile);
                }
            }
        }

        return writerToBuffNum;
    }

    public int init()
    {
        /* 检查工作子目录是否存在, 不存在则创建 */
        File fChildDir = new File(cacheDir);
        if (!fChildDir.exists())
        {
            if (!fChildDir.mkdirs())
            {
                log.info("cache dir[" + cacheDir + "] not-exist, and created fail");
                return OssConstants.RET_ERROR;
            }

            log.info("cache dir[" + cacheDir + "] not-exist, and created success");
        }
        else if ((!fChildDir.isDirectory()))
        {
            log.info("cache dir[" + cacheDir + "] exist, but not a directory, need check-and-handle by people.");
            return OssConstants.RET_ERROR;
        }

        /* 检查缓存文件信息 */

        return OssConstants.RET_OK;
    }

    public int size()
    {
        return buffSize();
    }

    /* 一次写一行, 由"\n"表示换行 */
    public int add(OssCacheData data)
    {
        int iRet;
        lock.lock();
        try
        {
            iRet = _add(data);
            if (RET_BUFF_FULL == iRet)
            {
                /* 将sharebuf中的记录作一次文件同步 */
                iRet = writeToFile();
                if (OssConstants.RET_ERROR == iRet)
                {
                    return iRet;
                }

                iRet = _add(data);
            }
        }
        finally
        {
            lock.unlock();
        }

        /* 其他异常, 目前不会发生 */
        if (RET_OK != iRet)
        {
            log.error("add data  fail, return " + iRet);
        }

        return RET_OK;
    }

    public OssCacheData get()
    {
        if (isBuffEmpty())
        {
            int readToBuffNum;
            lock.lock();
            try
            {
                /* 缓冲区空, 则尝试进行一次文件加载 */
                readToBuffNum = readFromFile();
            }
            finally
            {
                lock.unlock();
            }

            if (readToBuffNum <= 0)
            {
                return null;
            }
        }

        /* 如果此时还是get不到数据, 则就是没有数据 */
        OssCacheData objData = _get();
        if (null == objData)
        {
            log.error("get a valid buff-data, but objData=null, when writer-pos=%s, reader-pos=%s", writerPos, readerPos);
            return null;
        }

        return objData;
    }

    public int close()
    {
        return OssConstants.RET_ERROR;
    }

    public String toString()
    {
        StringBuilder sbInfo = new StringBuilder();
        sbInfo.append("\tcache-buff-max-size: " + maxBuffSize);
        sbInfo.append("\tcache-dir: " + cacheDir + "\n");

        sbInfo.append("\twriter-pos: " + writerPos + "\t");
        sbInfo.append("reader-pos: " + readerPos + "\t");
        sbInfo.append("cur-buff-size: " + buffSize() + "\n");

        File fCacheDir = new File(cacheDir);
        File[] lstCacheFile = fCacheDir.listFiles();
        if (null == lstCacheFile || lstCacheFile.length <= 0)
        {
            sbInfo.append("\tno cache files exist.");
        }
        else
        {
            for (File tmpCacheFile : lstCacheFile)
            {
                sbInfo.append("\t" + tmpCacheFile.toString() + "\n");
            }
        }

        return sbInfo.toString();
    }
}
