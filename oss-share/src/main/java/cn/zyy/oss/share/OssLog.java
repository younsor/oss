package cn.zyy.oss.share;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

public class OssLog
{
    public static String  LOG_MODULE_OSS  = "oss";
    public static String  LOG_MODULE_SRV  = "srv";
    public static String  LOG_MODULE_EXC1 = "srvexc1";
    public static String  LOG_MODULE_EXC2 = "srvexc2";
    public static String  LOG_MODULE_EXC3 = "srvexc3";
    private static String LOG_CONFIG_FILE = "logback.xml";

    private static String logbackXmlPath  = null;

    public static boolean initLogConfig()
    {
        File logbackFile = null;

        String logbackPath = System.getProperty("logback.configurationFile");
        if (null != logbackPath)
        {
            /* 如果有该配置, 则就使用配置的logback */
            logbackFile = new File(logbackPath);
            if (!logbackFile.exists())
            {
                /* 配置的文件不存在, 报错 */
                LoggerFactory.getLogger("LOG_MODULE_OSS").error(": init cannot find logback.xml for logback.configurationFile [" + logbackPath + "]");
                return false;
            }
        }
        else
        {
            /* 否则, 使用默认路径下的logback.xml */
            String curWorkDir = System.getProperty("user.dir");
            String logbackPath1 = curWorkDir + File.separatorChar + LOG_CONFIG_FILE;
            logbackFile = new File(logbackPath1);
            if (!logbackFile.exists())
            {
                String logbackPath2 = curWorkDir + File.separatorChar + "src" + File.separatorChar + "main" + File.separatorChar + "resources" + File.separatorChar + LOG_CONFIG_FILE;
                logbackFile = new File(logbackPath2);
                if (!logbackFile.exists())
                {
                    LoggerFactory.getLogger("LOG_MODULE_OSS").error(": init cannot find logback.xml in [" + logbackPath1 + "] or [" + logbackPath2 + "]");
                    return false;
                }
            }
        }

        /* 解析配置logback配置文件 */
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        try
        {
            configurator.doConfigure(logbackFile);
        }
        catch (JoranException e)
        {
            e.printStackTrace(System.err);
            LoggerFactory.getLogger("LOG_MODULE_OSS").error(": init parse " + logbackFile.getAbsolutePath() + " fail, and need handle");
            return false;
        }

        LoggerFactory.getLogger("LOG_MODULE_OSS").info(": init parse " + logbackFile.getAbsolutePath() + " success!");

        logbackXmlPath = logbackFile.getAbsolutePath();

        return true;
    }

    public static boolean resetLogConfig()
    {
        File logbackFile = new File(logbackXmlPath);
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        try
        {
            configurator.doConfigure(logbackFile);
        }
        catch (JoranException e)
        {
            e.printStackTrace(System.err);
            LoggerFactory.getLogger("LOG_MODULE_OSS").error("parse " + logbackFile.getPath() + " fail, and need handle");
            return false;
        }

        LoggerFactory.getLogger("LOG_MODULE_OSS").info(": reset parse " + logbackFile.getPath() + " success!");

        return true;
    }

    private Logger log = null;

    public OssLog()
    {
        this(LOG_MODULE_SRV);
    }

    public OssLog(String logModule)
    {
        log = LoggerFactory.getLogger(logModule);
    }

    public boolean isTraceEnabled()
    {
        return log.isTraceEnabled();
    }

    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    public void trace(String logInfo)
    {
        if (!log.isTraceEnabled())
        {
            return;
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(logInfo);

        log.trace(logBuilder.toString());
    }

    public void trace(String format, Object... objects)
    {
        if (!log.isTraceEnabled())
        {
            return;
        }

        StringBuilder formatBuilder = new StringBuilder(format);
        if (null != objects && objects.length > 0)
        {
            for (Object obj : objects)
            {
                int idxPos = formatBuilder.indexOf("%s");
                if (idxPos < 0)
                {
                    break;
                }
                formatBuilder.replace(idxPos, idxPos + 2, (null == obj) ? "null" : obj.toString());
            }
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(formatBuilder);

        log.trace(logBuilder.toString());
    }

    public void debug(String logInfo)
    {
        if (!log.isDebugEnabled())
        {
            return;
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(logInfo);

        log.debug(logBuilder.toString());
    }

    public void debug(String format, Object... objects)
    {
        if (!log.isDebugEnabled())
        {
            return;
        }

        StringBuilder formatBuilder = new StringBuilder(format);
        if (null != objects && objects.length > 0)
        {
            for (Object obj : objects)
            {
                int idxPos = formatBuilder.indexOf("%s");
                if (idxPos < 0)
                {
                    break;
                }
                formatBuilder.replace(idxPos, idxPos + 2, (null == obj) ? "null" : obj.toString());
            }
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(formatBuilder);

        log.debug(logBuilder.toString());
    }

    public void info(String logInfo)
    {
        if (!log.isInfoEnabled())
        {
            return;
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(logInfo);

        log.info(logBuilder.toString());
    }

    public void info(String format, Object... objects)
    {
        if (!log.isInfoEnabled())
        {
            return;
        }

        StringBuilder formatBuilder = new StringBuilder(format);
        if (null != objects && objects.length > 0)
        {
            for (Object obj : objects)
            {
                int idxPos = formatBuilder.indexOf("%s");
                if (idxPos < 0)
                {
                    break;
                }
                formatBuilder.replace(idxPos, idxPos + 2, (null == obj) ? "null" : obj.toString());
            }
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(formatBuilder);

        log.info(logBuilder.toString());
    }

    public void warn(String logInfo)
    {
        if (!log.isWarnEnabled())
        {
            return;
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(logInfo);

        log.warn(logBuilder.toString());
    }

    public void warn(String format, Object... objects)
    {
        if (!log.isWarnEnabled())
        {
            return;
        }

        StringBuilder formatBuilder = new StringBuilder(format);
        if (null != objects && objects.length > 0)
        {
            for (Object obj : objects)
            {
                int idxPos = formatBuilder.indexOf("%s");
                if (idxPos < 0)
                {
                    break;
                }
                formatBuilder.replace(idxPos, idxPos + 2, (null == obj) ? "null" : obj.toString());
            }
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(formatBuilder);

        log.warn(logBuilder.toString());
    }

    public void error(String logInfo)
    {
        if (!log.isErrorEnabled())
        {
            return;
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(logInfo);

        log.error(logBuilder.toString());
    }

    public void error(String format, Object... objects)
    {
        if (!log.isErrorEnabled())
        {
            return;
        }

        StringBuilder formatBuilder = new StringBuilder(format);
        if (null != objects && objects.length > 0)
        {
            for (Object obj : objects)
            {
                int idxPos = formatBuilder.indexOf("%s");
                if (idxPos < 0)
                {
                    break;
                }
                formatBuilder.replace(idxPos, idxPos + 2, (null == obj) ? "null" : obj.toString());
            }
        }

        StringBuilder logBuilder = new StringBuilder();
        StackTraceElement stackTraceInfo = Thread.currentThread().getStackTrace()[2];
        logBuilder.append("|");
        logBuilder.append(stackTraceInfo.getFileName());
        logBuilder.append("-");
        logBuilder.append(stackTraceInfo.getLineNumber());
        logBuilder.append("| ");
        logBuilder.append(formatBuilder);

        log.error(logBuilder.toString());
    }

    public Logger _origin_log()
    {
        return log;
    }
}
