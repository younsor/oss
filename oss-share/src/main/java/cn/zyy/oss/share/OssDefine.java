package cn.zyy.oss.share;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public class OssDefine
{
    public static class MapKey implements Comparable<MapKey>
    {
        public String              type     = "";
        public Map<String, String> mapField = Maps.newTreeMap();

        public MapKey(String type)
        {
            setType(type);
        }

        public void setType(String type)
        {
            if (null == type)
            {
                this.type = "";
            }
            else
            {
                this.type = type;
            }
        }

        public String getType()
        {
            return type;
        }

        public Set<String> getKeyFields()
        {
            return mapField.keySet();
        }

        public void put(String fieldName, String fieldValue)
        {
            mapField.put(fieldName, fieldValue);
        }

        public String get(String fieldName)
        {
            return mapField.get(fieldName);
        }

        public String toString()
        {
            String str = type;
            for (String fieldName : mapField.keySet())
            {
                str += ("|" + fieldName + "=" + mapField.get(fieldName));
            }

            return str;
        }

        public int getHashValue()
        {
            String strValue = toString();
            int hash = strValue.length();

            for (int i = 0; i < strValue.length(); i++)
            {
                hash = ((hash << 5) ^ (hash >> 27)) ^ strValue.charAt(i);
            }

            int hashValue = (hash & 0x7FFFFFFF);
            return hashValue;
        }

        @Override
        public int compareTo(MapKey that)
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
            if (null == obj || !(obj instanceof MapKey))
                throw new ClassCastException("类型异常");

            MapKey that = (MapKey) obj;
            return this.toString().equals(that.toString());
        }
    }

}
