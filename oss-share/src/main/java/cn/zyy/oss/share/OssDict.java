package cn.zyy.oss.share;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.zyy.oss.share.OssDB.SelectResult;

public class OssDict
{
    private static final OssLog                log                   = new OssLog();

    /****************************************字典中的业务ID定义************************************/
    public static final int                    FREQ_TYPE_NONE        = 0;
    public static final int                    FREQ_TYPE_IMP         = 1;
    public static final int                    FREQ_TYPE_CLK         = 2;
    public static final int                    FREQ_UNIT_NONE        = 0;
    public static final int                    FREQ_UNIT_DAY         = 1;
    public static final int                    FREQ_UNIT_FULL        = 2;

    public static final int                    CONTROL_TYPE_NONE     = 0;
    public static final int                    CONTROL_TYPE_COST     = 1;
    public static final int                    CONTROL_TYPE_IMP      = 2;

    public static final int                    SPEED_TYPE_UNIF       = 1;
    public static final int                    SPEED_TYPE_ACC        = 2;

    public static final int                    BUY_TYPE_RTB          = 1;
    public static final int                    BUY_TYPE_SCHE         = 2;

    public static final int                    AUDIT_OK              = 0;
    public static final int                    AUDIT_WAIT            = 1;
    public static final int                    AUDIT_REJECT          = 2;

    /***********************************配置类型(TYPE_CODE)定义************************************/
    public static final int                    TC_EMPTY              = 1;
    public static final int                    TC_AGE                = 2;
    public static final int                    TC_GENDER             = 3;
    public static final int                    TC_INDUSTRY           = 4;

    public static final int                    TC_MEDIA_CAT          = 5;
    public static final int                    TC_MEDIA_TAG          = 6;
    public static final int                    TC_TRAFFIC_SOURCE     = 7;
    public static final int                    TC_TRAFFIC_TYPE       = 8;
    public static final int                    TC_SLOT_TYPE          = 9;
    public static final int                    TC_CHARGE_TYPE        = 10;

    public static final int                    TC_SPEED              = 11;
    public static final int                    TC_TARGET_TYPE        = 12;
    public static final int                    TC_CARRIER_TYPE       = 13;
    public static final int                    TC_CONN_TYPE          = 14;
    public static final int                    TC_DVC_TYPE           = 15;

    public static final int                    TC_CONTROL_TYPE       = 16;
    public static final int                    TC_FREQ_TYPE          = 17;
    public static final int                    TC_FREQ_UNIT          = 18;
    public static final int                    TC_PROMTION_TYPE      = 19;
    public static final int                    TC_PURCHASE_TYPE      = 20;
    public static final int                    TC_QUALIFICATION_TYPE = 21;

    /* 以下暂时没有用到 */
    public static final int                    TC_ORDER_RUN_STATUS   = 22;
    public static final int                    TC_MEDIA_STATUS       = 23;
    public static final int                    TC_AD_SLOT_STATUS     = 24;
    public static final int                    TC_PLATFORM_STATUS    = 25;

    private static HashMap<Integer, String>    setTypeCode           = new HashMap<Integer, String>()
                                                                     {
                                                                         {
                                                                             put(TC_EMPTY, "empty");                                     /* 空定义 */
                                                                             put(TC_AGE, "age");                                         /* 年龄 */
                                                                             put(TC_GENDER, "gender");                                   /* 性别 */
                                                                             put(TC_INDUSTRY, "industry");                               /* 行业分类 */

                                                                             put(TC_MEDIA_CAT, "media_cat");                             /* 媒体分类 */
                                                                             put(TC_MEDIA_TAG, "media_tag");                             /* 媒体标签 */
                                                                             put(TC_TRAFFIC_SOURCE, "media_source");                     /* 媒体来源 */
                                                                             put(TC_TRAFFIC_TYPE, "traffic_type");                       /* 流量类型 */
                                                                             put(TC_SLOT_TYPE, "traffic_type");                          /* 流量类型 */
                                                                             put(TC_SLOT_TYPE, "adslot_adtype");                         /* 广告位类型 */
                                                                             put(TC_CHARGE_TYPE, "media_billtype");                      /* 媒体计费类型 */
                                                                             put(TC_SPEED, "advertising_speed");                         /* 投放速度 */
                                                                             put(TC_TARGET_TYPE, "target_type");                         /* 定向类型 */
                                                                             put(TC_CARRIER_TYPE, "carrier_type");                       /* 运营商类型 */
                                                                             put(TC_CONN_TYPE, "connection_type");                       /* 网络连接类型 */
                                                                             put(TC_DVC_TYPE, "device_type");                            /* 设备类型 */
                                                                             put(TC_CONTROL_TYPE, "control_type");                       /* 频次控制方式 */
                                                                             put(TC_FREQ_TYPE, "freq_control_type");                     /* 频次控制方式 */
                                                                             put(TC_FREQ_UNIT, "freq_unit");                             /* 频次控制周期粒度 */
                                                                             put(TC_PROMTION_TYPE, "promtion_type");                     /* 推广类型 */
                                                                             put(TC_PURCHASE_TYPE, "purchase_type");                     /* 购买方式 */
                                                                             put(TC_QUALIFICATION_TYPE, "qualification_type");           /* 资质类型 */

                                                                             put(TC_ORDER_RUN_STATUS, "run_status");                     /* 订单状态 */
                                                                             put(TC_MEDIA_STATUS, "media_status");                       /* 媒体状态 */
                                                                             put(TC_AD_SLOT_STATUS, "adslot_status");                    /* 广告位状态 */
                                                                             put(TC_PLATFORM_STATUS, "platform_status");                 /* 平台状态 */
                                                                         }
                                                                     };

    private Map<Integer, Map<String, Integer>> mapTypeCodeKeyId      = Maps.newHashMap();
    private Map<Integer, Map<String, Integer>> prev_mapTypeCodeKeyId = Maps.newHashMap();
    private Map<Integer, Map<Integer, String>> mapTypeCodeIdKey      = Maps.newHashMap();
    private Map<Integer, Map<Integer, String>> prev_mapTypeCodeIdKey = Maps.newHashMap();

    private Map<String, Integer>               mapKey2Id             = Maps.newHashMap();
    private Map<String, Integer>               prev_mapKey2Id        = Maps.newHashMap();

    public int updateDict(OssDB dbConn, String dbTableName)
    {
        SelectResult typeCodeResult = null;
        SelectResult keyResult = null;
        try
        {
            String selectSql = "select type_code, GROUP_CONCAT(`key`) as `keys` from " + dbTableName + " GROUP BY type_code";
            Set<String> setDimFields = Sets.newTreeSet();
            setDimFields.add("type_code");
            typeCodeResult = dbConn.selectRowSet(selectSql, setDimFields);

            selectSql = "select `key`, business_id from " + dbTableName;
            setDimFields.clear();
            setDimFields.add("key");
            keyResult = dbConn.selectRowSet(selectSql, setDimFields);
        }
        catch (Exception e)
        {
            log.error("load dict config exception\n" + OssFunc.getExceptionInfo(e));
            return OssConstants.RET_ERROR;
        }

        /* 解析typeCode相关的信息 */
        Map<Integer, Map<String, Integer>> tmpMapTypeKeyId = Maps.newHashMap();
        Map<Integer, Map<Integer, String>> tmpMapTypeIdKey = Maps.newHashMap();
        for (Integer iTypeCode : setTypeCode.keySet())
        {
            String strTypeCode = setTypeCode.get(iTypeCode);
            List<String> lstKey = typeCodeResult.getStringList("keys", "type_code", strTypeCode);
            if (null == lstKey || lstKey.size() <= 0)
            {
                continue;
            }

            Map<String, Integer> tmpMapKeyId = tmpMapTypeKeyId.get(strTypeCode);
            if (null == tmpMapKeyId)
            {
                tmpMapKeyId = Maps.newHashMap();
                tmpMapTypeKeyId.put(iTypeCode, tmpMapKeyId);
            }

            Map<Integer, String> tmpMapIdKey = tmpMapTypeIdKey.get(strTypeCode);
            if (null == tmpMapIdKey)
            {
                tmpMapIdKey = Maps.newHashMap();
                tmpMapTypeIdKey.put(iTypeCode, tmpMapIdKey);
            }

            for (String tmpKey : lstKey)
            {
                /* 加 key-->id 的映射关系 */
                Integer tmpId = keyResult.getInt(0, "business_id", "key", tmpKey);
                tmpMapKeyId.put(tmpKey, tmpId);

                /* 加 id-->key 的映射关系 */
                tmpMapIdKey.put(tmpId, tmpKey);
            }
        }

        /* key-->business信息 */
        Map<String, Integer> tmpMapKey2Id = Maps.newHashMap();
        Map<Map<String, String>, Map<String, Object>> tmpMapRecord = keyResult.getMapRecord();
        for (Map<String, String> tmpMapKey : tmpMapRecord.keySet())
        {
            Map<String, Object> tmpMapValue = tmpMapRecord.get(tmpMapKey);

            String tmpKey = tmpMapKey.get("key");
            if (null == tmpKey)
            {
                continue;
            }

            int tmpValue = OssFunc.DataConvert.toInt(tmpMapValue.get("business_id"), 0);

            if (tmpMapKey2Id.containsKey(tmpKey))
            {
                log.error("key[" + tmpKey + "] has always exist in dict-table");
            }

            tmpMapKey2Id.put(tmpKey, tmpValue);
        }

        /* 替换当前使用的字典配置 */
        prev_mapTypeCodeKeyId = mapTypeCodeKeyId;
        mapTypeCodeKeyId = tmpMapTypeKeyId;

        prev_mapTypeCodeIdKey = mapTypeCodeIdKey;
        mapTypeCodeIdKey = tmpMapTypeIdKey;

        prev_mapKey2Id = mapKey2Id;
        mapKey2Id = tmpMapKey2Id;

        return OssConstants.RET_OK;
    }

    public String id2Key(int typeCode, int srvId)
    {
        Map<Integer, String> mapIdKey = mapTypeCodeIdKey.get(typeCode);
        if (null == mapIdKey)
        {
            return null;
        }

        return mapIdKey.get(srvId);
    }

    public int key2Id(int typeCode, String strKey)
    {
        if (null == strKey)
        {
            return -1;
        }

        Map<String, Integer> mapIdKey = mapTypeCodeKeyId.get(typeCode);
        if (null == mapIdKey)
        {
            return -1;
        }

        Integer tmpSrvId = mapIdKey.get(strKey);
        if (null == tmpSrvId)
        {
            return -1;
        }

        return tmpSrvId;
    }

    public int key2Id(String strKey)
    {
        Integer iValue = mapKey2Id.get(strKey);
        if (null == iValue)
        {
            return 0;
        }

        return iValue;
    }

    public int key2Id(String strKey, int defId)
    {
        Integer iValue = mapKey2Id.get(strKey);
        if (null == iValue)
        {
            return defId;
        }

        return iValue;
    }
    
    public void close()
    {        
        if(null != mapTypeCodeKeyId)
        {
            mapTypeCodeKeyId.clear();
        }
        
        if(null != prev_mapTypeCodeKeyId)
        {
            prev_mapTypeCodeKeyId.clear();
        }
        
        if(null != mapTypeCodeIdKey)
        {
            mapTypeCodeIdKey.clear();
        }
        
        if(null != prev_mapTypeCodeIdKey)
        {
            prev_mapTypeCodeIdKey.clear();
        }
        
        if(null != mapKey2Id)
        {
            mapKey2Id.clear();
        }
        
        if(null != prev_mapKey2Id)
        {
            prev_mapKey2Id.clear();
        }
    }
}
