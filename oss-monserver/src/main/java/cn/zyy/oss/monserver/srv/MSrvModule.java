package cn.zyy.oss.monserver.srv;

import static org.httpkit.HttpUtils.HttpEncode;

import java.util.Date;

import org.httpkit.HeaderMap;

import cn.zyy.oss.core.proto.SysMonitor;
import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.core.task.OssTaskFsm;
import cn.zyy.oss.http.OssServer;
import cn.zyy.oss.monserver.main.MMonServer;
import cn.zyy.oss.monserver.share.MConstants;
import cn.zyy.oss.monserver.share.MSessInfo;
import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

public class MSrvModule extends OssTaskFsm
{
    private static final OssLog log       = new OssLog(OssLog.LOG_MODULE_OSS);
    private OssServer           monServer = null;

    public MSrvModule(int iPriority)
    {
        super(MConstants.TASK_NO_SRV, "srv", iPriority);
    }

    @Override
    public int init()
    {
        String tmpIp = MMonServer.getSysConfig().getServerIp();
        int tmpPort = MMonServer.getSysConfig().getServerPort();
        monServer = new OssServer(tmpIp, tmpPort, new GMonServerHandler(), 2);
        if (!monServer.start())
        {
            log.error("server start fail.");
            return MConstants.RET_ERROR;
        }

        log.info("monitor server start success. tmpIp=%s, tmpPort=%s", tmpIp, tmpPort);

        /* 设置定时器, 每分钟更新一次配置 */
        setTimer(OssCoreConstants.TIMER01, 600);

        return MConstants.RET_OK;
    }

    private void handlerMonInfo(SysMonitor.Info monInfo)
    {
        long secTime = monInfo.getSecondTime();
        for (SysMonitor.Perf perfInfo : monInfo.getMonPerfList())
        {
            StringBuilder printInfo = new StringBuilder();

            Date date = new Date(secTime * 1000);
            SysMonitor.Key monKey = perfInfo.getMonKey();
            String strKey = monKey.getHost() + "-" + monKey.getSystem() + "-" + monKey.getTask() + "-" + OssFunc.TimeConvert.Date2Format(date, "yyyy-MM-dd_HH:mm:ss") + ": ";
            String headInfo = String.format("%-60s", strKey);
            printInfo.append(headInfo);

            for (SysMonitor.AveValue valueInfo : perfInfo.getMonValueList())
            {
                String strValue = "";
                if (0 == valueInfo.getNum())
                {
                    strValue = "0(0)";
                }
                else
                {
                    long aveWeiMiao = valueInfo.getValue() / valueInfo.getNum() / 1000;
                    strValue = aveWeiMiao + "(" + valueInfo.getNum() + ")";
                }

                printInfo.append(String.format("%-15s", strValue));
            }

            MMonServer.perfLog()._origin_log().info(printInfo.toString());
        }
    }

    @Override
    protected void work(int msgId, Object objContext)
    {
        switch (msgId)
        {
            case MConstants.MSG_ID_SRV_RECV_MON:
            {
                if (null == objContext)
                {
                    log.error("null == objContext when msgId=MSG_ID_SRV_RECV_MON");
                    return;
                }

                if (!(objContext instanceof MSessInfo))
                {
                    log.error("objContext not instanceof SysMonitor.Info, objContext's class is" + objContext.getClass().getName());
                    return;
                }

                MSessInfo sessInfo = (MSessInfo) objContext;
                handlerMonInfo(sessInfo.monInfo);

                HeaderMap header = new HeaderMap();
                header.put("Connection", "Keep-Alive");
                sessInfo.callback.run(HttpEncode(200, header, ""));

                break;
            }

            case OssCoreConstants.MSG_ID_TIMER01:
            {
                setTimer(OssCoreConstants.TIMER01, 600);

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
        return MConstants.RET_OK;
    }
}
