package cn.zyy.oss.share;

import java.util.Map;

import com.google.common.collect.Maps;

public class OssSortKey implements Comparable<OssSortKey>
{
    public Map<String, Object> mapField = Maps.newTreeMap();

    private String _int2key(int sortSeq)
    {
        return String.format("%03d", sortSeq);
    }

    private int _key2int(String key)
    {
        return Integer.parseInt(key);
    }

    public void putSortInfo(int sortSeq, Object value)
    {
        mapField.put(_int2key(sortSeq), value);
    }

    public Object get(int sortSeq)
    {
        return mapField.get(_int2key(sortSeq));
    }

    @Override
    public int compareTo(OssSortKey that)
    {
        for (String key : mapField.keySet())
        {
            if (!that.mapField.containsKey(key))
            {
                return 1;
            }

            long lRet;
            Object thisValue = mapField.get(key);
            Object thatValue = that.mapField.get(key);
            if (!thisValue.getClass().getName().equals(thatValue.getClass().getName()))
            {
                lRet = thisValue.toString().compareTo(thatValue.toString());
            }
            else
            {
                if (thisValue instanceof Integer)
                {
                    lRet = (Integer) thisValue - (Integer) thatValue;
                }
                else if (thisValue instanceof Long)
                {
                    lRet = (Long) thisValue - (Long) thatValue;
                }
                else if (thisValue instanceof Float)
                {
                    Float tmpDiff = ((Float) thisValue - (Float) thatValue);
                    if (tmpDiff > 0.000001)
                    {
                        lRet = 1;
                    }
                    else if (tmpDiff < -0.000001)
                    {
                        lRet = -1;
                    }
                    else
                    {
                        lRet = 0;
                    }
                }
                else if (thisValue instanceof Double)
                {
                    Double tmpDiff = ((Double) thisValue - (Double) thatValue);
                    if (tmpDiff > 0.000000000000000000001)
                    {
                        lRet = 1;
                    }
                    else if (tmpDiff < -0.000000000000000000001)
                    {
                        lRet = -1;
                    }
                    else
                    {
                        lRet = 0;
                    }
                }
                else
                {
                    lRet = thisValue.toString().compareTo(thatValue.toString());
                }
            }

            if (0 != lRet)
            {
                return (lRet > 0) ? 1 : -1;
            }
        }

        return 0;
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
        if (null == obj || !(obj instanceof OssSortKey))
            throw new ClassCastException("类型异常");

        OssSortKey that = (OssSortKey) obj;
        for (String key : mapField.keySet())
        {
            if (!that.mapField.containsKey(key))
            {
                return false;
            }

            long lRet;
            Object thisValue = mapField.get(key);
            Object thatValue = that.mapField.get(key);
            if (!thisValue.getClass().getName().equals(thatValue.getClass().getName()))
            {
                lRet = thisValue.toString().compareTo(thatValue.toString());
            }
            else
            {
                if (thisValue instanceof Integer)
                {
                    lRet = (Integer) thisValue - (Integer) thatValue;
                }
                else if (thisValue instanceof Long)
                {
                    lRet = (Long) thisValue - (Long) thatValue;
                }
                else if (thisValue instanceof Float)
                {
                    Float tmpDiff = ((Float) thisValue - (Float) thatValue);
                    if (Math.abs(tmpDiff) < 0.000001)
                    {
                        lRet = 0;
                    }
                    else
                    {
                        lRet = 1;
                    }
                }
                else if (thisValue instanceof Double)
                {
                    Double tmpDiff = ((Double) thisValue - (Double) thatValue);
                    if (Math.abs(tmpDiff) < 0.000000000000000000001)
                    {
                        lRet = 0;
                    }
                    else
                    {
                        lRet = 1;
                    }
                }
                else
                {
                    lRet = thisValue.toString().compareTo(thatValue.toString());
                }
            }

            if (0 != lRet)
            {
                return false;
            }
        }

        return true;
    }

    public String toString()
    {
        return mapField.toString();
    }
}
