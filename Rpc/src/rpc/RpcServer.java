package rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import proto.RpcProto;

import com.google.protobuf.Service;

public class RpcServer {
	private Map<String, Service> serviceMap = new ConcurrentHashMap<String, Service>();
	private EventLoopGroup bossGroup = new NioEventLoopGroup();
	private EventLoopGroup workGroup = new NioEventLoopGroup();
	private ServerBootstrap bootstrap = new ServerBootstrap();
	private int port;
	
	public RpcServer(int port) {
		this.port = port;
	}
	
	public void register(Service service){
		serviceMap.put(service.getDescriptorForType().getFullName(), service);
	}
	
	public void start() throws InterruptedException{
		bootstrap.group(bossGroup, workGroup)
		.channel(NioServerSocketChannel.class)
		.childHandler(new ChannelInitializer<Channel>() {

			@Override
			protected void initChannel(Channel ch) throws Exception {
				// TODO Auto-generated method stub
				ChannelPipeline pipeline = ch.pipeline();
				
				pipeline.addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
				pipeline.addLast(new LengthFieldPrepender(4));
				
				pipeline.addLast(new ProtobufDecoder(RpcProto.RpcMessage.getDefaultInstance()));
				pipeline.addLast(new ProtobufEncoder());
				
				RpcCommonChannel rpcChannel = new RpcCommonChannel(ch);
				rpcChannel.setServiceMap(serviceMap);
				pipeline.addLast(new RpcChannelHandler(rpcChannel));
			} 
			
		})
		.option(ChannelOption.SO_BACKLOG, 128)
		.childOption(ChannelOption.SO_KEEPALIVE, true);
		try {
			bootstrap.bind(port).sync();
		} catch (InterruptedException e) {
			throw e;
		}
	}
	
	public void stop(){
		bossGroup.shutdownGracefully();
		workGroup.shutdownGracefully();
	}
}
