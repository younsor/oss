package org.httpkit.server;

import static java.nio.channels.SelectionKey.OP_ACCEPT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import cn.zyy.oss.share.OssLog;

public class HttpServer implements Runnable
{
    private static final OssLog       log          = new OssLog(OssLog.LOG_MODULE_OSS);

    private final String              ip;
    private final int                 port;
    private final IHandler            handler;
    private final Selector            selector;
    private final ServerSocketChannel serverChannel;

    private final String              threadName;
    private Thread                    serverThread;

    private final int                 nioThreadNum;
    private List<NioThread>           lstNioThread = new ArrayList<NioThread>();
    private long                      recvSocketNum;
    private AtomicLong                recvReqSeqno = new AtomicLong(1L);

    public HttpServer(String ip, int port, IHandler handler, int maxBody, int maxLine, int maxWs, int workThreadNum) throws IOException
    {
        this.ip = ip;
        this.port = port;
        this.handler = handler;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(this.ip, this.port), 10240);
        serverChannel.register(selector, OP_ACCEPT);

        threadName = "HttpServer" + this.port + "Accept";

        recvSocketNum = 0;
        nioThreadNum = workThreadNum;
        for (int i = 0; i < nioThreadNum; i++)
        {
            String tmpThreadName = "GServer" + this.port + "Work" + String.format("%02d", i);
            lstNioThread.add(new NioThread(this, tmpThreadName, handler, maxBody, maxLine, maxWs));
        }
    }

    public String getReqId()
    {
        return "HttpServer" + port + "-" + recvReqSeqno.getAndDecrement();
    }

    void accept(SelectionKey key)
    {
        ServerSocketChannel ch = (ServerSocketChannel) key.channel();
        SocketChannel s;
        try
        {
            while ((s = ch.accept()) != null)
            {
                /* 鎺ユ敹鍒癶ttp璇锋眰閾捐矾鍚� 灏嗛摼璺粦瀹氬埌涓�釜Work绾跨▼ */
                SocketAddress remoteAddress = s.getRemoteAddress();

                int idxNioThread = (int) (recvSocketNum % nioThreadNum);
                if (!lstNioThread.get(idxNioThread).addMsg(NioThread.NIOTHREAD_MSGID_001_BUNDING_SOCKET, s))
                {
                    log.error("socket(" + remoteAddress.toString() + ") bundling fail!");
                }
                else
                {
                    log.info("socket(" + remoteAddress.toString() + ") bundling success!");
                }

                recvSocketNum++;
            }
        }
        catch (Exception e)
        {
            log.error("accept coming but exception, info is: \n" + e.getStackTrace().toString());
        }
    }

    public void run()
    {
        while (true)
        {
            try
            {
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
                        accept(key);
                    }
                    else if (key.isReadable())
                    {
                        /* 寮傚父 */
                        log.error("recv read msg");
                    }
                    else if (key.isWritable())
                    {
                        /* 寮傚父 */
                        log.error("recv write msg");
                    }
                }
                selectedKeys.clear();
            }
            catch (ClosedSelectorException ignore)
            {
                return; // stopped
                // do not exits the while IO event loop. if exits, then will not
                // process any IO event
                // jvm can catch any exception, including OOM
            }
            catch (Throwable e)
            {
                // catch any exception(including OOM), print it
                log.error("http server loop error, should not happen, exception info is: \n" + e.getStackTrace().toString());
            }
        }
    }

    public void start() throws IOException
    {
        serverThread = new Thread(this, threadName);
        serverThread.start();

        for (int i = 0; i < nioThreadNum; i++)
        {
            lstNioThread.get(i).start();
        }
    }

    public void stop(int millTimeout)
    {
        try
        {
            serverChannel.close(); // stop accept any request
        }
        catch (IOException ignore)
        {
        }

        for (int i = 0; i < nioThreadNum; i++)
        {
            lstNioThread.get(i).addMsg(NioThread.NIOTHREAD_MSGID_002_CLOSE_THREAD, null);
        }

        handler.close(millTimeout);
    }

    public int getPort()
    {
        return this.serverChannel.socket().getLocalPort();
    }
}
