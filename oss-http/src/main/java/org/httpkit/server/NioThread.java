package org.httpkit.server;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static org.httpkit.HttpUtils.HttpEncode;
import static org.httpkit.HttpUtils.WsEncode;
import static org.httpkit.server.Frame.CloseFrame.CLOSE_AWAY;
import static org.httpkit.server.Frame.CloseFrame.CLOSE_MESG_BIG;
import static org.httpkit.server.Frame.CloseFrame.CLOSE_NORMAL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.httpkit.HeaderMap;
import org.httpkit.LineTooLargeException;
import org.httpkit.ProtocolException;
import org.httpkit.RequestTooLargeException;
import org.httpkit.server.Frame.BinaryFrame;
import org.httpkit.server.Frame.CloseFrame;
import org.httpkit.server.Frame.PingFrame;
import org.httpkit.server.Frame.PongFrame;
import org.httpkit.server.Frame.TextFrame;

import cn.zyy.oss.share.OssFunc;
import cn.zyy.oss.share.OssLog;

class PendingKey
{
    public final SelectionKey key;
    // operation: can be register for write or close the selectionkey
    public final int          Op;

    PendingKey(SelectionKey key, int op)
    {
        this.key = key;
        Op = op;
    }

    public static final int OP_WRITE = -1;
}

class NioMsg
{
    int    msgId;
    Object objMsg;

    NioMsg(int msgId, Object objMsg)
    {
        this.msgId = msgId;
        this.objMsg = objMsg;
    }
}

class NioThread implements Runnable
{
    private static final OssLog                 log                                = new OssLog(OssLog.LOG_MODULE_OSS);

    public static final int                     NIOTHREAD_MSGID_001_BUNDING_SOCKET = 1;
    public static final int                     NIOTHREAD_MSGID_002_CLOSE_THREAD   = 2;
    public static final int                     NIOTHREAD_MSGID_003_PENDING_OPR    = 3;

    private final IHandler                      handler;

    /* max http body size */
    private final int                           maxBody;

    /* max header line size */
    private final int                           maxLine;

    /* websocket max messagesize */
    private final int                           maxWs;
    private final Selector                      selector;

    private final String                        threadName;
    private Thread                              workThread;

    private final HttpServer                    belongHttpServer;

    /* 寮傛绾跨▼鎿嶄綔闃熷垪, 閲岄潰瀛樻斁鐫?渶瑕佸鐞嗙殑闈炲疄鏃剁綉缁淚/O浜嬩欢, 涓昏鏈変笁绫?
     * 1 寮傛绾跨▼鍏抽棴浜嬩欢
     * 2 寮傛绾跨▼缁戝畾http閾捐矾浜嬩欢
     * 3 寮傛绾跨▼鍦ㄥ凡寤洪摼璺笂鐨勪富鍔ㄦ搷浣滀簨浠?PendingKey涓殑鍚勭浜嬩欢: 寤惰繜鍐欍?闈瀔eep-alive閾捐矾鏂紑绛?..) */
    private final ConcurrentLinkedQueue<NioMsg> pending                            = new ConcurrentLinkedQueue<NioMsg>();

    /* shared, single thread */
    private final ByteBuffer                    buffer                             = ByteBuffer.allocateDirect(1024 * 64);

    public NioThread(HttpServer httpServer, String name, IHandler handler, int maxBody, int maxLine, int maxWs) throws IOException
    {
        this.belongHttpServer = httpServer;
        this.handler = handler;
        this.maxLine = maxLine;
        this.maxBody = maxBody;
        this.maxWs = maxWs;

        this.selector = Selector.open();
        threadName = name;
    }

    public void start()
    {
        workThread = new Thread(this, threadName);
        workThread.start();
    }

    public boolean addMsg(int msgId, Object objMsg)
    {
        if (pending.add(new NioMsg(msgId, objMsg)))
        {
            selector.wakeup();
            return true;
        }

        return false;
    }

    private void closeKey(final SelectionKey key, int status)
    {
        try
        {
            key.channel().close();
        }
        catch (Exception ignore)
        {
        }

        ServerAtta att = (ServerAtta) key.attachment();
        if (att instanceof HttpAtta)
        {
            handler.clientClose(att.channel, -1);
        }
        else if (att != null)
        {
            handler.clientClose(att.channel, status);
        }
    }

    private void decodeHttp(HttpAtta atta, SelectionKey key, SocketChannel ch)
    {
        try
        {
            do
            {
                AsyncChannel channel = atta.channel;
                HttpRequest request = atta.decoder.decode(buffer);
                if (request != null)
                {
                    channel.reset(request);
                    if (request.isWebSocket)
                    {
                        key.attach(new WsAtta(channel, maxWs));
                    }
                    else
                    {
                        atta.keepalive = request.isKeepAlive;
                    }
                    request.channel = channel;
                    request.remoteAddr = (InetSocketAddress) ch.socket().getRemoteSocketAddress();

                    String httpReqId = belongHttpServer.getReqId();
                    log.debug("recv http request(id=" + httpReqId + ")");

                    handler.handle(request, new RespCallback(key, this, httpReqId));
                    // pipelining not supported : need queue to ensure order
                    atta.decoder.reset();
                }
            }
            while (buffer.hasRemaining()); // consume all
        }
        catch (ProtocolException e)
        {
            closeKey(key, -1);
        }
        catch (RequestTooLargeException e)
        {
            atta.keepalive = false;
            tryWrite(key, "httpcode413", HttpEncode(413, new HeaderMap(), e.getMessage()));
        }
        catch (LineTooLargeException e)
        {
            atta.keepalive = false; // close after write
            tryWrite(key, "httpcode414", HttpEncode(414, new HeaderMap(), e.getMessage()));
        }
    }

    private void decodeWs(WsAtta atta, SelectionKey key)
    {
        try
        {
            do
            {
                Frame frame = atta.decoder.decode(buffer);
                if (frame instanceof TextFrame || frame instanceof BinaryFrame)
                {
                    handler.handle(atta.channel, frame);
                    atta.decoder.reset();
                }
                else if (frame instanceof PingFrame)
                {
                    atta.decoder.reset();
                    tryWrite(key, "ws", WsEncode(WSDecoder.OPCODE_PONG, frame.data));
                }
                else if (frame instanceof PongFrame)
                {
                    atta.decoder.reset();
                    tryWrite(key, "ws", WsEncode(WSDecoder.OPCODE_PING, frame.data));
                }
                else if (frame instanceof CloseFrame)
                {
                    handler.clientClose(atta.channel, ((CloseFrame) frame).getStatus());
                    // close the TCP connection after sent
                    atta.keepalive = false;
                    tryWrite(key, "ws", WsEncode(WSDecoder.OPCODE_CLOSE, frame.data));
                }
            }
            while (buffer.hasRemaining()); // consume all
        }
        catch (ProtocolException e)
        {
            System.err.printf("%s [%s] WARN - %s\n", new Date(), threadName, e.getMessage());
            closeKey(key, CLOSE_MESG_BIG); // TODO more specific error
        }
    }

    private void doRead(final SelectionKey key)
    {
        SocketChannel ch = (SocketChannel) key.channel();
        try
        {
            buffer.clear(); // clear for read
            int read = ch.read(buffer);
            if (read == -1)
            {
                // remote entity shut the socket down cleanly.
                closeKey(key, CLOSE_AWAY);
            }
            else if (read > 0)
            {
                buffer.flip(); // flip for read
                final ServerAtta atta = (ServerAtta) key.attachment();
                if (atta instanceof HttpAtta)
                {
                    decodeHttp((HttpAtta) atta, key, ch);
                }
                else
                {
                    decodeWs((WsAtta) atta, key);
                }
            }
        }
        catch (IOException e)
        { // the remote forcibly closed the connection
            closeKey(key, CLOSE_AWAY);
        }
    }

    private void doWrite(SelectionKey key)
    {
        ServerAtta atta = (ServerAtta) key.attachment();
        SocketChannel ch = (SocketChannel) key.channel();
        try
        {
            // the sync is per socket (per client). virtually, no contention
            // 1. keep byte data order, 2. ensure visibility
            synchronized (atta)
            {
                LinkedList<ByteBuffer> toWrites = atta.toWrites;
                int size = toWrites.size();
                if (size == 1)
                {
                    ch.write(toWrites.get(0));
                    // TODO investigate why needed.
                    // ws request for write, but has no data?
                }
                else if (size > 0)
                {
                    ByteBuffer buffers[] = new ByteBuffer[size];
                    toWrites.toArray(buffers);
                    ch.write(buffers, 0, buffers.length);
                }
                Iterator<ByteBuffer> ite = toWrites.iterator();
                while (ite.hasNext())
                {
                    if (!ite.next().hasRemaining())
                    {
                        ite.remove();
                    }
                }
                // all done
                if (toWrites.size() == 0)
                {
                    if (atta.isKeepAlive())
                    {
                        key.interestOps(OP_READ);
                    }
                    else
                    {
                        closeKey(key, CLOSE_NORMAL);
                    }
                }
            }
        }
        catch (IOException e)
        { // the remote forcibly closed the connection
            closeKey(key, CLOSE_AWAY);
        }
    }

    public void tryWrite(final SelectionKey key, String httpReqSeqId, ByteBuffer... buffers)
    {
        tryWrite(key, httpReqSeqId, false, buffers);
    }

    public void tryWrite(final SelectionKey key, String httpReqSeqId, boolean chunkInprogress, ByteBuffer... buffers)
    {
        ServerAtta atta = (ServerAtta) key.attachment();
        synchronized (atta)
        {
            SocketChannel ch = (SocketChannel) key.channel();
            atta.chunkedResponseInprogress(chunkInprogress);
            if (atta.toWrites.isEmpty())
            {
                try
                {
                    // TCP buffer most of time is empty, writable(8K ~ 256k)
                    // One IO thread => One thread reading + Many thread writing
                    // Save 2 system call
                    ch.write(buffers, 0, buffers.length);
                    log.debug("send http-rsp(" + httpReqSeqId + ") to SocketChannel");

                    if (buffers[buffers.length - 1].hasRemaining())
                    {
                        /* 寮傚父澶勭悊: 杩欎釜鎯呭喌浠?箞鏃跺?鍑虹幇鍛紵锛燂紵锛?鍚庨潰鐣欐剰 */
                        for (ByteBuffer b : buffers)
                        {
                            if (b.hasRemaining())
                            {
                                atta.toWrites.add(b);
                            }
                        }

                        addMsg(NIOTHREAD_MSGID_003_PENDING_OPR, new PendingKey(key, PendingKey.OP_WRITE));
                        selector.wakeup();
                    }
                    else if (!atta.isKeepAlive())
                    {
                        log.error("http link(" + ch.getRemoteAddress().toString() + ") not keep-alive, and close it.");

                        addMsg(NIOTHREAD_MSGID_003_PENDING_OPR, new PendingKey(key, CLOSE_NORMAL));
                    }
                }
                catch (IOException e)
                {
                    addMsg(NIOTHREAD_MSGID_003_PENDING_OPR, new PendingKey(key, CLOSE_AWAY));
                }
            }
            else
            {
                /* 寮傚父澶勭悊: 杩欎釜鎯呭喌浠?箞鏃跺?鍑虹幇鍛紵锛燂紵锛?鍚庨潰鐣欐剰 */
                log.debug("put http-rsp(" + httpReqSeqId + ") to SocketChannel's WriteBuffer");

                // If has pending write, order should be maintained. (WebSocket)
                Collections.addAll(atta.toWrites, buffers);
                addMsg(NIOTHREAD_MSGID_003_PENDING_OPR, new PendingKey(key, PendingKey.OP_WRITE));
                selector.wakeup();
            }
        }
    }

    private void closeThread()
    {
        /* 灏嗗紓姝ョ嚎绋嬬淮鎶ょ殑鎵?湁閾捐矾鍏抽棴鎺?*/
        if (selector.isOpen())
        {
            Set<SelectionKey> t = selector.keys();
            SelectionKey[] keys = t.toArray(new SelectionKey[t.size()]);
            for (SelectionKey k : keys)
            {
                /**
                 * 1. t.toArray will fill null if given array is larger.
                 * 2. compute t.size(), then try to fill the array, if in the mean time, another
                 *    thread close one SelectionKey, will result a NPE
                 *
                 * https://github.com/http-kit/http-kit/issues/125
                 */
                if (k != null)
                {
                    closeKey(k, 0); // 0 => close by server
                }
            }

            try
            {
                selector.close();
            }
            catch (IOException ignore)
            {
            }
        }
    }

    public String getThreadName()
    {
        return threadName;
    }

    public void run()
    {
        log.info(threadName + " start");
        while (true)
        {
            try
            {
                /* 鍏堝垽鏂湁鏈ㄦ湁寰呭鐞嗙殑闈炵綉璺疘O浜嬩欢 */
                NioMsg nioMsg;
                while ((nioMsg = pending.poll()) != null)
                {
                    switch (nioMsg.msgId)
                    {
                        case NIOTHREAD_MSGID_001_BUNDING_SOCKET:
                        {
                            if (!(nioMsg.objMsg instanceof SocketChannel))
                            {
                                log.error("nioMsg.objMsg's class type(" + nioMsg.objMsg.getClass().getName() + ") not SocketChannel");
                                break;
                            }

                            try
                            {
                                SocketChannel httpSocket = (SocketChannel) nioMsg.objMsg;
                                httpSocket.configureBlocking(false);
                                HttpAtta atta = new HttpAtta(maxBody, maxLine);
                                SelectionKey k = httpSocket.register(selector, OP_READ, atta);
                                atta.channel = new AsyncChannel(k, this);

                                log.debug("recv BUNDING_SOCKET msg and bunding http link(" + httpSocket.getRemoteAddress().toString() + ")");
                            }
                            catch (Exception e)
                            {
                                // eg: too many open files. do not quit
                                log.error("handler BUNDING_SOCKET msg exception info is: \n" + e.getStackTrace());
                            }

                            break;
                        }

                        case NIOTHREAD_MSGID_002_CLOSE_THREAD:
                        {
                            log.info("recv CLOSE_THREAD msg and closeThread");

                            closeThread();

                            log.info(threadName + " exit");
                            return;
                        }

                        case NIOTHREAD_MSGID_003_PENDING_OPR:
                        {
                            if (!(nioMsg.objMsg instanceof PendingKey))
                            {
                                log.error("nioMsg.objMsg's class type(" + nioMsg.objMsg.getClass().getName() + ") not PendingKey");
                                break;
                            }

                            PendingKey k = (PendingKey) nioMsg.objMsg;
                            if (k.Op == PendingKey.OP_WRITE)
                            {
                                if (k.key.isValid())
                                {
                                    k.key.interestOps(OP_WRITE);
                                }
                            }
                            else
                            {
                                closeKey(k.key, k.Op);
                            }

                            break;
                        }
                    }
                }

                if (selector.select() <= 0)
                {
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys)
                {
                    // TODO I do not know if this is needed
                    // if !valid, isAcceptable, isReadable.. will Exception
                    // run hours happily after commented, but not sure.
                    if (!key.isValid())
                    {
                        continue;
                    }

                    if (key.isAcceptable())
                    {
                        log.error("recv accept msg.");
                    }
                    else if (key.isReadable())
                    {
                        doRead(key);
                    }
                    else if (key.isWritable())
                    {
                        doWrite(key);
                    }
                }
                selectedKeys.clear();
            }
            catch (ClosedSelectorException ignore)
            {
                return;
                // stopped
                // do not exits the while IO event loop. if exits, then will not
                // process any IO event
                // jvm can catch any exception, including OOM
            }
            catch (Throwable e)
            {
                // catch any exception(including OOM), print it
                log.error("Throwable: \n " + OssFunc.getExceptionInfo(e));
            }
        }
    }
}
