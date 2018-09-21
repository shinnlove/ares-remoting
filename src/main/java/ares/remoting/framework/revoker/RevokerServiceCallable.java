package ares.remoting.framework.revoker;

import ares.remoting.framework.model.AresRequest;
import ares.remoting.framework.model.AresResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * RPC服务调用异步任务Task，所有的RPC服务调用都放在RPC服务线程池中进行。
 *
 * 当前任务Task使用`Netty Client`连接方式、对服务端发起异步IO的RPC调用，将结果集写入阻塞队列中等待获取。
 *
 * @author liyebing created on 17/2/10.
 * @version $Id$
 */
public class RevokerServiceCallable implements Callable<AresResponse> {

    private static final Logger logger = LoggerFactory.getLogger(RevokerServiceCallable.class);

    private Channel channel;
    private InetSocketAddress inetSocketAddress;
    private AresRequest request;

    public static RevokerServiceCallable of(InetSocketAddress inetSocketAddress, AresRequest request) {
        return new RevokerServiceCallable(inetSocketAddress, request);
    }


    public RevokerServiceCallable(InetSocketAddress inetSocketAddress, AresRequest request) {
        this.inetSocketAddress = inetSocketAddress;
        this.request = request;
    }

    @Override
    public AresResponse call() throws Exception {
        //初始化返回结果容器,将本次调用的唯一标识作为Key存入返回结果的Map
        RevokerResponseHolder.initResponseData(request.getUniqueKey());
        //根据本地调用服务提供者地址获取对应的Netty通道channel队列
        ArrayBlockingQueue<Channel> blockingQueue = NettyChannelPoolFactory.channelPoolFactoryInstance().acquire(inetSocketAddress);
        try {
            if (channel == null) {
                //从队列中获取本次调用的Netty通道channel
                channel = blockingQueue.poll(request.getInvokeTimeout(), TimeUnit.MILLISECONDS);
            }

            //若获取的channel通道已经不可用,则重新获取一个
            while (!channel.isOpen() || !channel.isActive() || !channel.isWritable()) {
                logger.warn("----------retry get new Channel------------");
                channel = blockingQueue.poll(request.getInvokeTimeout(), TimeUnit.MILLISECONDS);
                if (channel == null) {
                    //若队列中没有可用的Channel,则重新注册一个Channel
                    // 若注册失败，直接抛NPE，会进入catch中，不太合适(后期可以改造下)
                    channel = NettyChannelPoolFactory.channelPoolFactoryInstance().registerChannel(inetSocketAddress);
                }
            }

            /**
             * ========客户端调用线程Task========
             * 特别注意，因为Netty底层是异步的，而发起请求调用是同步的——指的是在给定的时间内、阻塞线程、直到返回成功|失败|超时这样的结果。
             * 而Netty底层是异步，所以客户端发起同步调用`异转同`需要做如下几件事：
             *
             * 1、`调用线程`把服务请求信息写入通道中(一旦写入通道中，被NIO线程接管，剩下是同步等待的事情)、并同步等待通道写入成功(含消息编码、`Netty用户缓冲区`写入`系统网卡驱动缓冲区`)；
             * 2、`调用线程`从`UUID/TraceId映射`的`RPC响应结果阻塞队列`中获取服务调用结果(poll till timeout)；
             *
             * ==========以下内容为异步执行（不在调用线程中的客户端与服务端交互）==========
             * 当服务端接收到消息后，会进行解码、反射调用服务、封装调用结果写入通道、编码等步骤，消息会发送到客户端。
             * 此时客户端接收到调用结果后，将会从`React从线程组`中选出一根`NIO线程`:
             *  串行化调用`NettyClientInvokeHandler`进行解码、将服务调用结果写入`UUID/TraceId映射`的`RPC响应结果阻塞队列`中，并带上时间戳!!!
             *
             * ========客户端调用线程Task========
             * `调用线程`Task如果在timeout时间段内、从结果队列中取出了结果，则将结果集返回给上层调用，否则超时。
             */

            // 将本次调用的信息写入Netty通道,发起异步调用
            ChannelFuture channelFuture = channel.writeAndFlush(request);
            channelFuture.syncUninterruptibly();

            //从返回结果容器中获取返回结果,同时设置等待超时时间为invokeTimeout
            long invokeTimeout = request.getInvokeTimeout();
            return RevokerResponseHolder.getValue(request.getUniqueKey(), invokeTimeout);

        } catch (Exception e) {
            logger.error("service invoke error.", e);
        } finally {
            //本次调用完毕后,将Netty的通道channel重新释放到队列中,以便下次调用复用
            NettyChannelPoolFactory.channelPoolFactoryInstance().release(blockingQueue, channel, inetSocketAddress);
        }
        return null;
    }
}
