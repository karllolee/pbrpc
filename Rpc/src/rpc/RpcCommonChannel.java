package rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import proto.RpcProto.ErrorCode;
import proto.RpcProto.MessageType;
import proto.RpcProto.RpcMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class RpcCommonChannel implements BlockingRpcChannel, RpcChannel {

	private static final class AsyncPair{
		public Message responsePrototype;
		public RpcCallback<Message> done;
		
		public AsyncPair(Message responsePrototype, RpcCallback<Message> done){
			this.responsePrototype = responsePrototype;
			this.done = done;
		}
	}
	
	private Channel channel;
	private AtomicInteger channelId = new AtomicInteger(1);
	private Map<Integer, AsyncPair> responseMap = new ConcurrentHashMap<Integer, AsyncPair>();
	private Map<String, Service> serviceMap;
	
	public RpcCommonChannel(Channel channel) {
		// TODO Auto-generated constructor stub
		this.channel = channel;
	}
	
	public Channel getChannel() {
		return channel;
	}
	
	public void setServiceMap(Map<String, Service> serviceMap) {
		this.serviceMap = serviceMap;
	}
	
	private void handleRequest(RpcMessage msg){
		final int id = msg.getId();	
		Service service = serviceMap.get(msg.getService());
		RpcMessage.Builder err = RpcMessage.newBuilder();
		err.setType(MessageType.ERROR);
		err.setId(id);
		if (service != null){
			
			MethodDescriptor method = service.getDescriptorForType().findMethodByName(msg.getMethod());

			if (method != null){
				Message requestProtoType = service.getRequestPrototype(method);
				ByteString requeString = msg.getRequest();
				Message request = null;
				try {
					request = requestProtoType.toBuilder().mergeFrom(requeString).build();
				} catch (InvalidProtocolBufferException e) {
					err.setError(ErrorCode.BAD_REQUEST);
					return;
				}
				
				
				RpcCallback<Message> done = new RpcCallback<Message>() {

					@Override
					public void run(Message response) {
						// TODO Auto-generated method stub
						if (response != null){
							RpcMessage res = RpcMessage.newBuilder()
									.setType(MessageType.RESPONSE)
									.setId(id)
									.setResponse(response.toByteString())
									.build();
							channel.writeAndFlush(res);
						}else{
							RpcMessage res = RpcMessage.newBuilder()
									.setType(MessageType.ERROR)
									.setId(id)
									.setError(ErrorCode.BAD_RESPONSE)
									.build();
							channel.writeAndFlush(res);
						}
					}
				};

				service.callMethod(method, null, request, done);
				return;
			}else{
				err.setError(ErrorCode.NO_METHOD);
			}
		}else{
			err.setError(ErrorCode.NO_SERVICE);
		}
		channel.writeAndFlush(err.build());
	}
	
	private void handleResponse(RpcMessage msg) {
		int id = msg.getId();
		AsyncPair pair = responseMap.remove(id);
		if (pair == null){
			System.err.println("no such id: " + id);
			return;
		}
		Message response = null;
		try {
			response = pair.responsePrototype.toBuilder()
					.mergeFrom(msg.getResponse()).build();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pair.done.run(response);
	}
	
	private void handleError(RpcMessage msg) {
		ErrorCode errorCode = msg.getError();
		switch (errorCode.getNumber()) {
		case ErrorCode.NO_SERVICE_VALUE:
			System.err.println("no service");
			break;
		case ErrorCode.NO_METHOD_VALUE:
			System.err.println("no method");
			break;
		case ErrorCode.BAD_REQUEST_VALUE:
			System.err.println("bad request");
			break;
		case ErrorCode.BAD_RESPONSE_VALUE:
			System.err.println("bad response");
			break;
		default:
			System.err.println("other error");
			break;
		}
	}
	public void channelRead(ChannelHandlerContext ctx, RpcMessage msg){
		
		if (msg.getType() == MessageType.REQUEST){
			handleRequest(msg);
		}else if (msg.getType() == MessageType.RESPONSE){
			handleResponse(msg);
		}else {
			handleError(msg);
		}
	}
	
	@Override
	public void callMethod(MethodDescriptor method, RpcController controller,
			Message request, Message responsePrototype,
			RpcCallback<Message> done) {
		// TODO Auto-generated method stub
		int id = channelId.getAndIncrement();
		RpcMessage req = RpcMessage.newBuilder()
				.setType(MessageType.REQUEST)
				.setId(id)
				.setService(method.getService().getFullName())
				.setMethod(method.getName())
				.setRequest(request.toByteString())
				.build();
		responseMap.put(id, new AsyncPair(responsePrototype, done));
		channel.writeAndFlush(req);

	}

	private static final class BlockingCallback implements RpcCallback<Message>{

		public Message response = null;
		@Override
		public void run(Message response) {
			// TODO Auto-generated method stub
			synchronized(this){
				this.response = response;
				this.notify();
			}
		}
		
	}
	
	@Override
	public Message callBlockingMethod(MethodDescriptor method,
			RpcController controller, Message request, Message responsePrototype)
			throws ServiceException {
		// TODO Auto-generated method stub
		BlockingCallback done = new BlockingCallback();
		callMethod(method, null, request, responsePrototype, done);
		synchronized (done) {
			while(done.response == null){
				try {
					done.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		return done.response;
	}

}
