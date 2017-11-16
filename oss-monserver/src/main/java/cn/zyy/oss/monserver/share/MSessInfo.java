package cn.zyy.oss.monserver.share;

import org.httpkit.server.RespCallback;

import cn.zyy.oss.core.proto.SysMonitor;

public class MSessInfo
{
    public SysMonitor.Info monInfo  = null;
    public RespCallback    callback = null;
}
