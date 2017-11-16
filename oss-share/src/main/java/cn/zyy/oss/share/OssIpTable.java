package cn.zyy.oss.share;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.google.common.collect.Lists;

public class OssIpTable
{
    private static final OssLog log = new OssLog();

    private class Record
    {
        public long   start;
        public long   over;
        public long   regionId;
        public double lat;
        public double lon;
    };

    private String       verInfo     = null;
    private List<Record> lstIpRecord = Lists.newArrayList();

    public static class RGGeo
    {
        public long   region;
        public double lat;
        public double lon;
    }

    public OssIpTable(String ver)
    {
        verInfo = ver;
    }

    public int parse(String ipFilePath)
    {
        try
        {
            String lineTxt = null;
            InputStreamReader read = new InputStreamReader(new FileInputStream(ipFilePath));
            BufferedReader bufferedReader = new BufferedReader(read);

            while ((lineTxt = bufferedReader.readLine()) != null)
            {
                /* 必须能被","号分成4断 */
                lineTxt = lineTxt.trim();
                String[] aInfo = lineTxt.split(",");
                if (null == aInfo || 5 != aInfo.length)
                {
                    log.error("parse file[" + ipFilePath + "] exist a error-line[" + lineTxt + "]");

                    bufferedReader.close();
                    read.close();
                    return OssConstants.RET_ERROR;
                }

                long tmpStart = Long.parseLong(aInfo[0].trim());
                long tmpOver = Long.parseLong(aInfo[1].trim());
                long tmpRegionId = Long.parseLong(aInfo[2].trim());
                double tmpLat = Double.parseDouble(aInfo[3].trim());
                double tmpLon = Double.parseDouble(aInfo[4].trim());

                Record record = new Record();
                record.start = tmpStart;
                record.over = tmpOver;
                record.regionId = tmpRegionId;
                record.lat = tmpLat;
                record.lon = tmpLon;

                lstIpRecord.add(record);
            }

            bufferedReader.close();
            read.close();
        }
        catch (Exception e)
        {
            log.error("parser ip file exception\n" + OssFunc.getExceptionInfo(e));

            return OssConstants.RET_ERROR;
        }

        return OssConstants.RET_OK;
    }

    public long getRegionId(long ipValue)
    {
        int tmpIdxStart = 0;
        int tmpIdxOver = lstIpRecord.size() - 1;
        if (tmpIdxStart > tmpIdxOver)
        {
            return 0;
        }

        int midStart = 0;
        while (tmpIdxStart <= tmpIdxOver)
        {
            midStart = (tmpIdxStart + tmpIdxOver) / 2;
            if (ipValue >= lstIpRecord.get(midStart).start && ipValue <= lstIpRecord.get(midStart).over)
            {
                return lstIpRecord.get(midStart).regionId;
            }
            else if (ipValue < lstIpRecord.get(midStart).start)
            {
                tmpIdxOver = midStart - 1;
            }
            else if (ipValue > lstIpRecord.get(midStart).over)
            {
                tmpIdxStart = midStart + 1;
            }
        }

        return 0;
    }

    public RGGeo getRGGeo(long ipValue)
    {
        int tmpIdxStart = 0;
        int tmpIdxOver = lstIpRecord.size() - 1;
        if (tmpIdxStart > tmpIdxOver)
        {
            return null;
        }

        int midStart = 0;
        while (tmpIdxStart <= tmpIdxOver)
        {
            midStart = (tmpIdxStart + tmpIdxOver) / 2;
            if (ipValue >= lstIpRecord.get(midStart).start && ipValue <= lstIpRecord.get(midStart).over)
            {
                RGGeo rgGeo = new RGGeo();
                rgGeo.region = lstIpRecord.get(midStart).regionId;
                rgGeo.lat = lstIpRecord.get(midStart).lat;
                rgGeo.lon = lstIpRecord.get(midStart).lon;
                return rgGeo;
            }
            else if (ipValue < lstIpRecord.get(midStart).start)
            {
                tmpIdxOver = midStart - 1;
            }
            else if (ipValue > lstIpRecord.get(midStart).over)
            {
                tmpIdxStart = midStart + 1;
            }
        }

        return null;
    }

    public long getRegionId(String ipStr)
    {
        return getRegionId(OssFunc.ipToLong(ipStr));
    }

    public RGGeo getRGGeo(String ipStr)
    {
        RGGeo tmpRgGeo = null;
        try
        {
            tmpRgGeo = getRGGeo(OssFunc.ipToLong(ipStr));
        }
        catch (Exception e)
        {
            log.error("parse ip[" + ipStr + "] exception:\n" + OssFunc.getExceptionInfo(e));
            tmpRgGeo = null;
        }

        return tmpRgGeo;
    }
}
