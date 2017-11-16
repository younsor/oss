package cn.zyy.oss.flog.file;

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

import cn.zyy.oss.flog.share.OssFlogConstants;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class OssShareCache
{
    private static final OssLog log                     = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int    RECORD_NUM_PER_FLUSH    = 20000;
    private static final int    RECORD_NUM_PER_READ     = 20000;
    private static final int    CACHE_FILE_MAX_SIZE_KB  = 51200;
    private static final String CACHE_FILE_NAME_PREFIX  = "cachefile";
    private static final String CACHE_FILE_NAME_POSTFIX = "yyyyMMdd-HHmmss-SSS";

    public static final byte[]  byteDelimiter           = "\n".getBytes();
    public static final int     RET_ERROR               = -1;
    public static final int     RET_OK                  = 0;
    public static final int     RET_NO_DATA             = 1;
    public static final int     RET_BUFF_FULL           = 2;
    public static final int     RET_BUFF_EMPTY          = 3;
    public final static int     RET_NO_CACHE_FILE       = 4;

    private ReentrantLock       lock                    = null;
    private final int           maxBuffSize;
    private Object[]            arrayBuff               = null;
    private int                 writerPos;                                                  /* 当前可写位置, 该位置无数据 */
    private int                 readerPos;                                                  /* 当前可读位置, 该位置有数据 */

    /* 日志索引目录, 与flog任务索引一致, 由此可定位一个子目录 */
    private String              workDir                 = null;
    private String              cacheName               = null;
    private String              cacheDirPath            = null;

    public OssShareCache(String name, String dir, int initBuffSize)
    {
        maxBuffSize = initBuffSize;
        arrayBuff = new Object[maxBuffSize + 1];
        lock = new ReentrantLock();
        writerPos = 0;
        readerPos = 0;

        workDir = dir.trim();
        cacheName = name;
        cacheDirPath = cacheDirPath();
    }

    private String cacheDirPath()
    {
        String childDir;
        if (workDir.charAt(workDir.length() - 1) == '/' || workDir.charAt(workDir.length() - 1) == '\\')
        {
            childDir = workDir + cacheName;
        }
        else
        {
            childDir = workDir + "/" + cacheName;
        }

        return childDir;
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

    private int _add(Object data)
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

    private Object _get()
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

    /* 写回退接口, 回退num个数据出来, num为最大回退数
     * 线程不安全接口, 此时接口外围已经lock住
     * writerPos不会变动
     * readerPos会变动 */
    private Object[] rollback(int num)
    {
        /* 回退的原因是队列满了, 需要回退出一些消息进行文件备份, 因此不需要回退完; 否则会读、写缓存文件, 造成资源浪费
         * 队列中最起码要预留20%长度的数据 */
        int maxRollbackSize = buffSize() - (arrayBuff.length / 10);
        if (maxRollbackSize <= 0)
        {
            return null;
        }

        int rollbackNum = (maxRollbackSize > num) ? num : maxRollbackSize;

        /* 需要迁移走的数据是从rollbackWriterPos 到 curWriterPos - 1 */
        int curWriterPos = writerPos;
        int rollbackWriterPos = (writerPos - rollbackNum + maxBuffSize + 1) % (maxBuffSize + 1);

        /* 先重置writerPos */
        writerPos = rollbackWriterPos;

        Object[] arrayRollbackData = new Object[rollbackNum];
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
        return CACHE_FILE_NAME_PREFIX + postFix;
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
        File fCacheDir = new File(cacheDirPath);
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
            if (fileSizeKB >= CACHE_FILE_MAX_SIZE_KB)
            {
                /* 如果最有一个文件大小已达限, 则新启动一个文件 */
                isNeedNewFile = true;
            }
        }

        File fWriterFile = null;
        if (isNeedNewFile)
        {
            String cachaFileName = newCacheFileName();
            String cachaFilePath = cacheDirPath + "/" + cachaFileName;
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
        Object[] rollbackData = rollback(RECORD_NUM_PER_FLUSH);
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
                byte[] byteRecord = (byte[]) rollbackData[idxPos];

                /* byteRecord末尾自带了换行 */
                String strRecord = new String(byteRecord);
                writerBuff.write(strRecord);
                // writerBuff.newLine();
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
        File fCacheDir = new File(cacheDirPath);
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
            log.error("readFromFile(%s) fail", fReadFile);
            return OssFlogConstants.RET_FLOG_ERROR;
        }

        int readLineNum;
        int initReadLineNo;
        if (fileContent.size() <= RECORD_NUM_PER_READ)
        {
            readLineNum = fileContent.size();
            initReadLineNo = 0;
        }
        else
        {
            readLineNum = RECORD_NUM_PER_READ;
            initReadLineNo = fileContent.size() - RECORD_NUM_PER_READ;
        }

        /* 把从 initReadLineNo 到 fileContent.size()-1 的数据写到共享缓冲区 */
        for (int idx = 0; idx < readLineNum; idx++)
        {
            String strRecord = fileContent.get(initReadLineNo);
            byte[] byteRecord = strRecord.getBytes();

            byte[] byteSendRecord = new byte[byteRecord.length + byteDelimiter.length];
            System.arraycopy(byteRecord, 0, byteSendRecord, 0, byteRecord.length);
            System.arraycopy(byteDelimiter, 0, byteSendRecord, byteRecord.length, byteDelimiter.length);
            int iRet = _add(byteSendRecord);
            if (OssFlogConstants.RET_FLOG_OK != iRet)
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
        /* 工作目录不存在则报错 */
        File fWorkDir = new File(workDir);
        if ((!fWorkDir.exists()) || (!fWorkDir.isDirectory()))
        {
            log.error("flume log cache dir not-exist or not-directory, and must be created!");
            return OssConstants.RET_ERROR;
        }

        /* 检查工作子目录是否存在, 不存在则创建 */
        File fChildDir = new File(cacheDirPath);
        if (!fChildDir.exists())
        {
            if (!fChildDir.mkdirs())
            {
                log.error("create child-cache-dir(%s) fail", cacheDirPath);
                return OssConstants.RET_ERROR;
            }
        }
        else if ((!fChildDir.isDirectory()))
        {
            log.error("child-cache-dir path(%s) exist, but not a directory, need check-and-handle by people.", cacheDirPath);
            return OssConstants.RET_ERROR;
        }

        /* 检查缓存文件信息 */

        return OssConstants.RET_OK;
    }

    /* 一次写一行, 由"\n"表示换行 */
    public int add(byte[] data)
    {
        int iRet;
        lock.lock();
        try
        {
            iRet = _add(data);
            if (RET_BUFF_FULL == iRet)
            {
                /* 将shrebuf中的记录作一次文件同步 */
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
            log.error("add(data) fail, return " + iRet);
        }

        return RET_OK;
    }

    public Object get()
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
        Object objData = _get();
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
        sbInfo.append("\tcache-dir: " + cacheDirPath + "\n");

        sbInfo.append("\twriter-pos: " + writerPos + "\t");
        sbInfo.append("reader-pos: " + readerPos + "\t");
        sbInfo.append("cur-buff-size: " + buffSize() + "\n");

        File fCacheDir = new File(cacheDirPath);
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
