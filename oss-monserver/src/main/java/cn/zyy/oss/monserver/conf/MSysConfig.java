package cn.zyy.oss.monserver.conf;

import cn.zyy.oss.share.OssConfig;
import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssLog;

public class MSysConfig
{
    private static final OssLog log             = new OssLog(OssLog.LOG_MODULE_OSS);
    private static final int    DEF_SERVER_PORT = 11111;

    private String              confFile        = null;

    /* 服务端通信节点配置 */
    private String              serverIp        = null;
    private int                 serverPort      = 0;

    public MSysConfig(String file)
    {
        confFile = file;
    }

    public int resolveConfig()
    {
        serverIp = OssConfig.getStringValue(confFile, "server_ip", "");
        if ("".equals(serverIp))
        {
            log.error("config: server_ip error.");
            return OssConstants.RET_ERROR;
        }

        serverPort = OssConfig.getIntValue(confFile, "server_port", DEF_SERVER_PORT);
        if (0 == serverPort)
        {
            log.error("config: server_port error.");
            return OssConstants.RET_ERROR;
        }

        return OssConstants.RET_OK;
    }

    public String getServerIp()
    {
        return serverIp;
    }

    public int getServerPort()
    {
        return serverPort;
    }

    public String toString()
    {
        String outInfo = "";
        outInfo += "config file: " + confFile + "\n";

        outInfo += "server_ip: " + serverIp + "\n";
        outInfo += "server_port: " + serverPort + "\n";

        return outInfo;
    }
}
