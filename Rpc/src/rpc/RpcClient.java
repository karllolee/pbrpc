package rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import proto.RpcProto;


public class RpcClient {

	private Bootstrap bootstrap = new Bootstrap();
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	private RpcCommonChannel rpcChannel;
	
	public RpcClient(){
		bootstrap.group(workerGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				// TODO Auto-generated method stub
				ChannelPipeline pipeline = ch.pipeline();
				
				pipeline.addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
				pipeline.addLast(new LengthFieldPrepender(4));
				
				pipeline.addLast(new ProtobufDecoder(RpcProto.RpcMessage.getDefaultInstance()));
				pipeline.addLast(new ProtobufEncoder());
				
				rpcChannel = new RpcCommonChannel(ch);
				ch.pipeline().addLast(new RpcChannelHandler(rpcChannel));
			}
		});
	}
	
	public void asyncConnect(SocketAddress addr, RpcCommonCallback done) throws Exception{
		ChannelFuture future = bootstrap.connect(addr);
		future.addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if (future.isSuccess()){
					done.run(rpcChannel);
				}
				else {
					throw new Exception("connect error");
				}
			}
		});
		
	}
	
	public RpcCommonChannel syncConnect(SocketAddress addr, long timeout, TimeUnit unit) throws Exception, InterruptedException{
		final CountDownLatch latch = new CountDownLatch(1);
		asyncConnect(addr, new RpcCommonCallback() {
			
			@Override
			public void run(RpcCommonChannel channel) {
				// TODO Auto-generated method stub
				latch.countDown();
			}
		});
		try{
			latch.await(timeout, unit);
		}catch (InterruptedException e){
			throw e;
		}
		return rpcChannel;
	}
	
	public void close(){
		workerGroup.shutdownGracefully();
	}
}
