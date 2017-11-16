package org.httpkit.server;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RespCallback
{
    private final SelectionKey key;
    private final NioThread    server;
    private String             httpReqSeqId;

    public RespCallback(SelectionKey key, NioThread server, String reqSeqId)
    {
        this.key = key;
        this.server = server;
        this.httpReqSeqId = reqSeqId;
    }

    // maybe in another thread :worker thread
    public void run(ByteBuffer... buffers)
    {
        server.tryWrite(key, httpReqSeqId, buffers);
    }

    public String getHttpReqId()
    {
        return httpReqSeqId;
    }
}
