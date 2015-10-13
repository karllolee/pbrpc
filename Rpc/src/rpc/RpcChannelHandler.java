package rpc;

import proto.RpcProto.RpcMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class RpcChannelHandler extends SimpleChannelInboundHandler<RpcMessage> {

	private RpcCommonChannel rpcChannel;
	public RpcChannelHandler(RpcCommonChannel rpcChannel) {
		// TODO Auto-generated constructor stub
		this.rpcChannel = rpcChannel;
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, RpcMessage msg)
			throws Exception {
		// TODO Auto-generated method stub
		rpcChannel.channelRead(ctx, msg);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
		cause.printStackTrace();
		ctx.close();
	}
	
	

}
