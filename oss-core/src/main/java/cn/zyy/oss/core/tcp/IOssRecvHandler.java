package cn.zyy.oss.core.tcp;

import org.apache.commons.codec.binary.Base64;

import cn.zyy.oss.share.OssConstants;
import cn.zyy.oss.share.OssLog;

public abstract class IOssRecvHandler
{
    private static final OssLog log                      = new OssLog();
    private static int          RECV_BUFF_SIZE           = 2 * 1024 * 1024;

    protected static final int  RET_LINK_ERROR           = -2;
    protected static final int  RET_HANDLER_NOT_COMPLETE = -1;
    protected static final int  RET_HANDLER_OK           = 0;

    public static final int     TCP_CLT_MSG_HEAD         = 0x0c27701d;
    public static final int     TCP_SRV_MSG_HEAD         = 0x4b414c52;

    public static byte[] getCltMsgHead(int msgLen)
    {
        byte[] byteHead8 = new byte[8];
        byteHead8[0] = 0x0c;
        byteHead8[1] = 0x27;
        byteHead8[2] = 0x70;
        byteHead8[3] = 0x1d;

        int totalLen = msgLen + 8;
        byteHead8[4] = (byte) ((totalLen >> 24) & 0x000000FF);
        byteHead8[5] = (byte) ((totalLen >> 16) & 0x000000FF);
        byteHead8[6] = (byte) ((totalLen >> 8) & 0x000000FF);
        byteHead8[7] = (byte) (totalLen & 0x000000FF);

        return byteHead8;
    }

    public static byte[] getSrvMsgHead(int msgLen)
    {
        byte[] byteHead8 = new byte[8];
        byteHead8[0] = 0x4b;
        byteHead8[1] = 0x41;
        byteHead8[2] = 0x4c;
        byteHead8[3] = 0x52;

        int totalLen = msgLen + 8;
        byteHead8[4] = (byte) ((totalLen >> 24) & 0x000000FF);
        byteHead8[5] = (byte) ((totalLen >> 16) & 0x000000FF);
        byteHead8[6] = (byte) ((totalLen >> 8) & 0x000000FF);
        byteHead8[7] = (byte) (totalLen & 0x000000FF);

        return byteHead8;
    }

    private boolean isClientRole = true;
    private int     linkIdx      = OssConstants.RET_ERROR;

    public byte[]   recvBuff     = new byte[RECV_BUFF_SIZE + 1];
    public int      writePos     = 0;
    public int      readPos      = 0;

    public void setRoleTrueClt_FalseSrv(boolean isClient)
    {
        if (isClient)
        {
            isClientRole = true;
        }
        else
        {
            isClientRole = false;
        }
    }

    public void setLinkIdx(int linkIdx)
    {
        this.linkIdx = linkIdx;
    }

    public int getLinkIdx()
    {
        return linkIdx;
    }

    private int getCheckHead()
    {
        if (isClientRole)
        {
            return TCP_SRV_MSG_HEAD;
        }
        else
        {
            return TCP_CLT_MSG_HEAD;
        }
    }

    private String getFormatCheckHead()
    {
        if (isClientRole)
        {
            return "0x0c27701d";
        }
        else
        {
            return "0x4b414c52";
        }
    }

    /** 检查消息头及消息长度, 返回值情况如下: 
     * 1) >0: 表示消息头校验、长度都正确; 返回有效长度
     * 2) =0: 表示未达到校验及消息长度的判断标准, 继续等待
     * 3) <0: 表示消息头校验、长度判断异常, 说明接收链路码流异常, 需要断链处理 
     * */
    public int checkHeadAndGetMsgLen()
    {
        int curDataSize = dataSize();
        if (curDataSize <= 8)
        {
            return RET_HANDLER_NOT_COMPLETE;
        }

        byte[] head8 = getData(8);
        if (null == head8)
        {
            log.error("get 8 bytes error");
            return RET_LINK_ERROR;
        }

        int headValue = (int) ((head8[0] << 24 & 0xFF000000) | ((head8[1] << 16) & 0x00FF0000) | ((head8[2] << 8) & 0x0000FF00) | (head8[3] & 0x000000FF));
        if (getCheckHead() != headValue)
        {
            String exceptionHead = String.format("0x%x%x%x%x", head8[0], head8[1], head8[2], head8[3]);
            log.error("recv raw exception: head4=" + exceptionHead + " != " + getFormatCheckHead());
            return RET_LINK_ERROR;
        }

        int msgLen = (int) ((head8[4] << 24 & 0xFF000000) | ((head8[5] << 16) & 0x00FF0000) | ((head8[6] << 8) & 0x0000FF00) | (head8[7] & 0x000000FF));
        if (msgLen <= 8 || msgLen >= maxBuffSize() - 8)
        {
            log.error("recv raw exception: msgLen[" + msgLen + "] >= maxBuffSize[" + (maxBuffSize() - 8) + "]");
            return RET_LINK_ERROR;
        }

        if (curDataSize < msgLen)
        {
            log.trace("cur msg-len=%s, but data-size=%s, and continue prepare");
            return RET_HANDLER_NOT_COMPLETE;
        }

        return msgLen;
    }

    public int maxBuffSize()
    {
        return RECV_BUFF_SIZE;
    }

    public int dataSize()
    {
        int size = writePos - readPos;
        if (size >= 0)
        {
            return size;
        }
        else
        {
            return size + RECV_BUFF_SIZE + 1;
        }
    }

    public int buffSize()
    {
        int size = writePos - readPos;
        if (size >= 0)
        {
            return RECV_BUFF_SIZE - size;
        }
        else
        {
            return (0 - size - 1);
        }
    }

    public int buffSize4OnceRecv()
    {
        int buffLen = buffSize();
        int writePos2EndLen = recvBuff.length - writePos;
        return (buffLen < writePos2EndLen) ? buffLen : writePos2EndLen;
    }

    public int writePos()
    {
        return writePos;
    }

    public int writeByteNum(int num)
    {
        writePos = (writePos + num) % recvBuff.length;
        return writePos;
    }

    public byte[] getData(int num)
    {
        int curDataSize = dataSize();
        if (num <= 0 || num > curDataSize)
        {
            log.error("get data's length[" + num + "] invalid, cur-data-size=" + curDataSize);
            return null;
        }

        int tmpReadPos = readPos;
        byte[] readData = new byte[num];
        for (int idx = 0; idx < num; idx++)
        {
            readData[idx] = recvBuff[tmpReadPos];
            tmpReadPos = (tmpReadPos + 1) % recvBuff.length;
        }

        return readData;
    }

    public byte[] removeData(int num)
    {
        int curDataSize = dataSize();
        if (num <= 0 || num > curDataSize)
        {
            log.error("get data's length[" + num + "] invalid, cur-data-size=" + curDataSize);
            return null;
        }

        byte[] removeData = new byte[num];
        for (int idx = 0; idx < num; idx++)
        {
            removeData[idx] = recvBuff[readPos];
            readPos = (readPos + 1) % recvBuff.length;
        }

        return removeData;
    }

    public String toString()
    {
        return "BuffInfo[total-len=" + recvBuff.length + ", write-pos=" + writePos + ", read-pos" + readPos + "]";
    }

    public String getBuffRawBase64String(int startPos, int endPos)
    {
        StringBuilder strBuff = new StringBuilder();

        int size = endPos - startPos;
        if (size < 0)
        {
            size = size + RECV_BUFF_SIZE + 1;
        }

        byte[] byteBuff = new byte[size];

        for (int idx = 0; idx < byteBuff.length; idx++)
        {
            byteBuff[idx] = recvBuff[startPos];
            startPos = (startPos + 1) % recvBuff.length;
        }

        strBuff.append(Base64.encodeBase64String(byteBuff));
        strBuff.append("\n");
        strBuff.append("size=" + size);

        return strBuff.toString();
    }

    public abstract int onHandler();
}
