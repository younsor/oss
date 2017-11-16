package cn.zyy.oss.share;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public class OssReport
{
    private static final OssLog log = new OssLog();

    public static class ReportKey implements Comparable<ReportKey>
    {
        private String              name     = "";
        private int                 type;
        private Map<String, String> mapField = Maps.newTreeMap();

        public ReportKey(String argName, int type)
        {
            if (null == argName)
            {
                this.name = "";
            }
            else
            {
                this.name = argName;
            }

            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public int getType()
        {
            return type;
        }

        public Set<String> getKeyFields()
        {
            return mapField.keySet();
        }

        public void put(String fieldName, String fieldValue)
        {
            String tmpKey = (null == fieldName) ? "" : fieldName.trim();
            String tmpValue = (null == fieldValue) ? "" : fieldValue.trim();
            mapField.put(tmpKey, tmpValue);
        }

        public String get(String fieldName)
        {
            return mapField.get(fieldName);
        }

        public boolean exist(String fieldName)
        {
            return mapField.containsKey(fieldName);
        }

        public void delete(String fieldName)
        {
            mapField.remove(fieldName);
        }

        public String toString()
        {
            StringBuffer str = new StringBuffer();
            str.append(name + "|" + type);

            for (String fieldName : mapField.keySet())
            {
                str.append("|" + fieldName + "=" + mapField.get(fieldName));
            }

            return str.toString();
        }

        public ReportKey getCopy()
        {
            ReportKey copyKey = new ReportKey(this.name, this.type);
            for (String key : mapField.keySet())
            {
                copyKey.put(key, mapField.get(key));
            }

            return copyKey;
        }

        @Override
        public int compareTo(ReportKey that)
        {
            return this.toString().compareTo(that.toString());
        }

        @Override
        public int hashCode()
        {
            String strValue = toString();
            int hash = 0;
            int len = strValue.length();
            for (int off = 0; off < len; off++)
            {
                hash = 31 * hash + strValue.charAt(off++);
            }

            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (null == obj || !(obj instanceof ReportKey))
                throw new ClassCastException("类型异常");

            ReportKey that = (ReportKey) obj;
            return this.toString().equals(that.toString());
        }
    }

    public static class ReportValue
    {
        private Map<String, BaseTarget> targets = Maps.newHashMap();

        public Set<String> getKeyFields()
        {
            return targets.keySet();
        }

        public BaseTarget get(String fieldName)
        {
            return targets.get(fieldName);
        }

        public void put(String valueName, BaseTarget value)
        {
            BaseTarget curValue = targets.get(valueName);
            if (null == curValue)
            {
                targets.put(valueName, value);
            }
            else
            {
                /* 已经存在值了 */
                curValue.merge(value);
            }
        }

        public Map<String, BaseTarget> merge(ReportValue thatTargets)
        {
            for (String thatKey : thatTargets.targets.keySet())
            {
                BaseTarget thatValue = thatTargets.targets.get(thatKey);

                BaseTarget thisValue = targets.get(thatKey);
                if (null == thisValue)
                {
                    /* 没有该值, 直接复制过去 */
                    targets.put(thatKey, thatValue);
                }
                else
                {
                    /* 有该值, 则进行合并 */
                    try
                    {
                        thisValue.merge(thatValue);
                    }
                    catch (Exception e)
                    {
                        log.error(OssFunc.getExceptionInfo(e));
                    }
                }
            }

            return targets;
        }

        public String toString()
        {
            StringBuilder strBuff = new StringBuilder();
            for (String strKey : targets.keySet())
            {
                if (strBuff.length() > 0)
                {
                    strBuff.append("|");
                }

                strBuff.append(strKey + "=" + targets.get(strKey).toString());
            }

            return strBuff.toString();
        }
    }

    public static abstract class BaseTarget
    {
        public abstract void merge(BaseTarget value);

        public abstract String format();

        public abstract String toString();
    }

    public static class SumTarget extends BaseTarget
    {
        public long value;

        public SumTarget(long value)
        {
            this.value = value;
        }

        @Override
        public void merge(BaseTarget thatValue)
        {
            if (!(thatValue instanceof SumTarget))
            {
                log.error("thatValue[" + thatValue.getClass().getName() + "] not instanceof SumValue, so cannot merge");
                return;
            }

            value += ((SumTarget) thatValue).value;
        }

        @Override
        public String format()
        {
            return "" + value;
        }

        @Override
        public String toString()
        {
            return "" + value;
        }
    }
}
