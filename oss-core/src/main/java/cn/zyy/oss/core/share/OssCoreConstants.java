package cn.zyy.oss.core.share;

import cn.zyy.oss.share.OssConstants;

public class OssCoreConstants
{
    public final static String ENV_RUN_INFO_FILE             = "ZYY_RUN_INFO_FILE";
    public final static String DEF_RUN_INFO_FILE             = ".zyy_run_info";

    public final static String RUN_INFO_INIT_OK              = "init ok";
    public final static String RUN_INFO_QUIT_OK              = "quit ok";

    public final static String DATE_FORMAT_FOR_RMQ           = "yyyy-MM-dd HH:mm:ss";

    /* 系统状态 */
    public final static int    SYS_STATUS_INIT               = 1;
    public final static int    SYS_STATUS_WORK               = 2;
    public final static int    SYS_STATUS_CLOSE              = 3;
    public final static int    SYS_STATUS_EXIT               = 4;

    /* 任务类型 */
    public final static int    TASK_TYPE_PRODUCER            = 1;
    public final static int    TASK_TYPE_CONSUMER            = 2;
    public final static int    TASK_TYPE_FSM                 = 3;

    /* 系统预留返回码: 系统预留错误码(-1~999),
     * -1: 错误
     * 0:  成功, (0~999)为支撑系统预留 
     * 1000及以上, 为业务系统预留 
     * */
    public final static int    RET_ERROR                     = OssConstants.RET_ERROR;
    public final static int    RET_OK                        = OssConstants.RET_OK;
    public final static int    RET_OSS_PARAM_ERROR           = OssConstants.RET_OSS_START + 1;
    public final static int    RET_OSS_TCB_FULL              = OssConstants.RET_OSS_START + 2;
    public final static int    RET_OSS_TCB_IDLE_FULL         = OssConstants.RET_OSS_START + 3;
    public final static int    RET_OSS_TASK_INVALID          = OssConstants.RET_OSS_START + 4;
    public final static int    RET_OSS_MSG_QUEUE_FULL        = OssConstants.RET_OSS_START + 5;
    public final static int    RET_OSS_MSG_QUEUE_ERROR       = OssConstants.RET_OSS_START + 6;
    public final static int    RET_OSS_MSG_BLOCL_TMO         = OssConstants.RET_OSS_START + 7;

    /* 系统任务号 
     * 1: 1~199为支撑系统预留 
     * 2: 200~499为支撑核心通用能力预留, 此类功能组建一般也放在oss中, 如: flumelog 
     * 3: 500~1999及以上为应用系统预留 
     * */
    public final static int    TNO_INVALID                   = -1;
    public final static int    TNO_START                     = 1;

    public final static int    TNO_FSM_START                 = TNO_START + 0;
    public final static int    TNO_FSM_OSS_START             = TNO_FSM_START + 0;
    public final static int    TNO_FSM_OSS_MGR               = TNO_FSM_OSS_START + 0;
    public final static int    TNO_FSM_OSS_TIMER             = TNO_FSM_OSS_START + 1;
    public final static int    TNO_FSM_OSS_MON               = TNO_FSM_OSS_START + 2;

    public final static int    TNO_FSM_CORE_START            = 200;
    public final static int    TNO_FSM_CORE_FLOG_START       = TNO_FSM_CORE_START + 0;
    public final static int    TNO_FSM_CORE_FLOG_END         = TNO_FSM_CORE_START + 19;

    public final static int    TNO_FSM_SRV_START             = 500;

    public final static int    TNO_FSM_END                   = 1000;

    public final static int    TNO_TASK_START                = 1001;
    public final static int    TNO_TASK_END                  = 2000;

    public final static int    TNO_OVER                      = 2000;

    /* 定时器编号标示 */
    public final static int    MIN_TIMER_NO                  = 1;
    public final static int    TIMER01                       = 1;
    public final static int    TIMER02                       = 2;
    public final static int    TIMER03                       = 3;
    public final static int    TIMER04                       = 4;
    public final static int    TIMER05                       = 5;
    public final static int    TIMER06                       = 6;
    public final static int    TIMER07                       = 7;
    public final static int    TIMER08                       = 8;
    public final static int    TIMER09                       = 9;
    public final static int    TIMER10                       = 10;
    public final static int    TIMER11                       = 11;
    public final static int    TIMER12                       = 12;
    public final static int    TIMER13                       = 13;
    public final static int    TIMER14                       = 14;
    public final static int    TIMER15                       = 15;
    public final static int    TIMER16                       = 16;
    public final static int    TIMER17                       = 17;
    public final static int    TIMER18                       = 18;
    public final static int    TIMER19                       = 19;
    public final static int    TIMER20                       = 20;
    public final static int    TIMER21                       = 21;
    public final static int    TIMER22                       = 22;
    public final static int    TIMER23                       = 23;
    public final static int    TIMER24                       = 24;
    public final static int    TIMER25                       = 25;
    public final static int    TIMER26                       = 26;
    public final static int    TIMER27                       = 27;
    public final static int    TIMER28                       = 28;
    public final static int    TIMER29                       = 29;
    public final static int    TIMER30                       = 30;
    public final static int    TIMER31                       = 31;
    public final static int    TIMER32                       = 32;
    public final static int    MAX_TIMER_NO                  = TIMER32;

    /* 系统消息ID
     * -777: 强退进程, 没什么意思, 就这么定一个值 
     * 0~999: 系统预留消息ID段
     * 1000~以上: 业务层预留消息ID段 */
    public final static int    MSG_ID_TASK_KILL              = -777;
    public final static int    MSG_ID_INVALID                = 0;
    public final static int    MSG_ID_TASK_INIT              = 3;
    public final static int    MSG_ID_TASK_WORK              = 4;
    public final static int    MSG_ID_TASK_CLOSE             = 5;
    public final static int    MSG_ID_RMQ_DATA               = 6;
    public final static int    MSG_ID_SEND_MON_MSG           = 7;
    public final static int    MSG_ID_TCP_CLOSE              = 15;
    public final static int    MSG_ID_TCP_SEND               = 16;
    public final static int    MSG_ID_TCP_RECV               = 17;
    public final static int    MSG_ID_TCP_ACCEPT             = 18;

    public final static int    MSG_ID_TIMER_MSG_START        = 20;
    public final static int    MSG_ID_TIMER01                = MSG_ID_TIMER_MSG_START + 1;
    public final static int    MSG_ID_TIMER02                = MSG_ID_TIMER_MSG_START + 2;
    public final static int    MSG_ID_TIMER03                = MSG_ID_TIMER_MSG_START + 3;
    public final static int    MSG_ID_TIMER04                = MSG_ID_TIMER_MSG_START + 4;
    public final static int    MSG_ID_TIMER05                = MSG_ID_TIMER_MSG_START + 5;
    public final static int    MSG_ID_TIMER06                = MSG_ID_TIMER_MSG_START + 6;
    public final static int    MSG_ID_TIMER07                = MSG_ID_TIMER_MSG_START + 7;
    public final static int    MSG_ID_TIMER08                = MSG_ID_TIMER_MSG_START + 8;
    public final static int    MSG_ID_TIMER09                = MSG_ID_TIMER_MSG_START + 9;
    public final static int    MSG_ID_TIMER10                = MSG_ID_TIMER_MSG_START + 10;
    public final static int    MSG_ID_TIMER11                = MSG_ID_TIMER_MSG_START + 11;
    public final static int    MSG_ID_TIMER12                = MSG_ID_TIMER_MSG_START + 12;
    public final static int    MSG_ID_TIMER13                = MSG_ID_TIMER_MSG_START + 13;
    public final static int    MSG_ID_TIMER14                = MSG_ID_TIMER_MSG_START + 14;
    public final static int    MSG_ID_TIMER15                = MSG_ID_TIMER_MSG_START + 15;
    public final static int    MSG_ID_TIMER16                = MSG_ID_TIMER_MSG_START + 16;
    public final static int    MSG_ID_TIMER17                = MSG_ID_TIMER_MSG_START + 17;
    public final static int    MSG_ID_TIMER18                = MSG_ID_TIMER_MSG_START + 18;
    public final static int    MSG_ID_TIMER19                = MSG_ID_TIMER_MSG_START + 19;
    public final static int    MSG_ID_TIMER20                = MSG_ID_TIMER_MSG_START + 20;
    public final static int    MSG_ID_TIMER21                = MSG_ID_TIMER_MSG_START + 21;
    public final static int    MSG_ID_TIMER22                = MSG_ID_TIMER_MSG_START + 22;
    public final static int    MSG_ID_TIMER23                = MSG_ID_TIMER_MSG_START + 23;
    public final static int    MSG_ID_TIMER24                = MSG_ID_TIMER_MSG_START + 24;
    public final static int    MSG_ID_TIMER25                = MSG_ID_TIMER_MSG_START + 25;
    public final static int    MSG_ID_TIMER26                = MSG_ID_TIMER_MSG_START + 26;
    public final static int    MSG_ID_TIMER27                = MSG_ID_TIMER_MSG_START + 27;
    public final static int    MSG_ID_TIMER28                = MSG_ID_TIMER_MSG_START + 28;
    public final static int    MSG_ID_TIMER29                = MSG_ID_TIMER_MSG_START + 29;
    public final static int    MSG_ID_TIMER30                = MSG_ID_TIMER_MSG_START + 30;
    public final static int    MSG_ID_TIMER31                = MSG_ID_TIMER_MSG_START + 31;
    public final static int    MSG_ID_TIMER32                = MSG_ID_TIMER_MSG_START + 32;
    public final static int    MSG_ID_TIMER_MSG_OVER         = MSG_ID_TIMER32;

    public final static int    MSG_ID_FLOG_START             = 200;

    public final static int    MSG_ID_SRV_START              = 1000;

    /* 监控系统定义 */
    public static final String MON_TYPE                      = "type";
    public static final String MON_KEY_FIELD_HOST            = "host";
    public static final String MON_KEY_FIELD_SYSTEM          = "system";
    public static final String MON_KEY_FIELD_TASK            = "task";

    public static final int    TIME_ACCURACY_10_MILL_SECOND  = 1;
    public static final int    TIME_ACCURACY_100_MILL_SECOND = 2;
    public static final int    TIME_ACCURACY_1_SECOND        = 3;
}
