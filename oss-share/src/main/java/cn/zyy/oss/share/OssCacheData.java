package cn.zyy.oss.share;

public abstract class OssCacheData
{
    public OssCacheData()
    {}

    public abstract String serialString();

    public abstract OssCacheData deserialFromString(String strFormat);
}
