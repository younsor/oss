package cn.zyy.oss.flog.share;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.share.OssConstants;

public class OssFlogConstants
{
    public final static int    MSG_ID_OSS_FLOG_RECV      = OssCoreConstants.MSG_ID_FLOG_START + 0;

    public final static int    RET_FLOG_ERROR            = OssConstants.RET_ERROR;
    public final static int    RET_FLOG_OK               = OssConstants.RET_OK;
    public final static int    RET_FLOG_SHARE_BUFF_EMPTY = 1;
    public final static int    RET_FLOG_NO_CACHE_FILE    = 2;

    public final static String SEQNO_FILE                = ".seqno_file";
    public final static String CACHE_FILE_NAME_PREFIX    = "flumelog";
    public static int          LOG_PRIORITY              = 133;
}
