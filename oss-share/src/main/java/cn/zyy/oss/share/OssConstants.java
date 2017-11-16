package cn.zyy.oss.share;

public class OssConstants
{
    /* 系统预留返回码: 系统预留错误码(-1~999),
     * -1: 错误
     * 0:  成功, (0~999)为支撑系统预留 
     * 1000及以上, 为业务系统预留 
     * */
    public final static int    RET_EXCEPTION          = -2;
    public final static int    RET_ERROR              = -1;
    public final static int    RET_OK                 = 0;
    public final static int    RET_OSS_START          = 1;
    public final static int    RET_SRV_START          = 1001;

    public final static int    HTTP_REQ_TYPE_GET      = 0;
    public final static int    HTTP_REQ_TYPE_POST     = 1;
    public final static int    HTTP_REQ_TYPE_OTHER    = 2;

    /* 任务优先级, 系统定义10级优先级, 从0~9; 0是最高优先级, 9是最低优先级
     * 系统启动时: 按照由高->低的顺序启动任务
     * 系统退出时: 按照由低->高的顺序启动任务
     *  */
    public final static int    TASK_PRIORITY_0        = 0;
    public final static int    TASK_PRIORITY_1        = 1;
    public final static int    TASK_PRIORITY_2        = 2;
    public final static int    TASK_PRIORITY_3        = 3;
    public final static int    TASK_PRIORITY_4        = 4;
    public final static int    TASK_PRIORITY_5        = 5;
    public final static int    TASK_PRIORITY_6        = 6;
    public final static int    TASK_PRIORITY_7        = 7;
    public final static int    TASK_PRIORITY_8        = 8;
    public final static int    TASK_PRIORITY_9        = 9;
    public final static int    TASK_PRIORITY_MAX      = TASK_PRIORITY_0;
    public final static int    TASK_PRIORITY_MIN      = TASK_PRIORITY_9;

    public final static String MTEK_RECV_MILLTIME     = "mtek-recv-milltime";
    public final static String MTEK_RECV_NANOTIME     = "mtek-recv-nanotime";

    /* 全系统的公共定义 */
    public final static String HTTP_HEAD_CLT_IP_FIELD = "x-forwarded-for";
}
