package cn.zyy.oss.monserver.share;

import cn.zyy.oss.core.share.OssCoreConstants;
import cn.zyy.oss.share.OssConstants;

public class MConstants
{
    /* 系统预留返回码: -1-错误, 0-成功, 1~199. 200~以上预留给业务系统 */
    public final static int RET_OK              = OssConstants.RET_OK;
    public final static int RET_ERROR           = OssConstants.RET_ERROR;

    /* 业务系统消息ID, 从MOssConstants.MSG_ID_SRV_START开始 */
    public final static int MSG_ID_SRV_RECV_MON = OssCoreConstants.MSG_ID_SRV_START + 1;

    /* 业务系统任务号, 其中 TASK_NO_SRV ~ TASK_NO_SRV+128 是业务系统任务号 */
    public final static int TASK_NO_CONF        = OssCoreConstants.TNO_FSM_SRV_START + 0;
    public final static int TASK_NO_SRV         = OssCoreConstants.TNO_FSM_SRV_START + 1;
}
