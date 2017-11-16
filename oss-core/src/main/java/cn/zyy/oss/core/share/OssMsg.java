package cn.zyy.oss.core.share;

public class OssMsg
{
    public int    srcTno     = OssCoreConstants.TNO_INVALID;
    public int    destTno    = OssCoreConstants.TNO_INVALID;
    public int    msgId      = OssCoreConstants.MSG_ID_INVALID;
    public Object objContext = null;

    public boolean isCloseMsg()
    {
        return (OssCoreConstants.MSG_ID_TASK_CLOSE == msgId);
    }
}
