import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;


public class MyServer {

	public FileStoreHandler handler;
	public FileStore.Processor<FileStore.Iface> processor;
	public int portNo;
	public String iPAddress;

	public static void main(String[] args) {
		try {
			MyServer server = new MyServer();
			server.setData(server, args[0]);
			Runnable activity = new Runnable() {
				@Override
				public void run() {
					try {
						server.startServer();
					} catch (TTransportException e) {
						e.printStackTrace();
					}
				}
			};
			Thread thread = new Thread(activity);
			thread.start();
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Error in input, please pass the port no.");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	private void setData(MyServer server, String portNum) throws UnknownHostException
	{
		server.setPortNo(Integer.parseInt(portNum));
		server.setiPAddress(InetAddress.getLocalHost().getHostAddress());
		server.setHandler(new FileStoreHandler(iPAddress, portNo));
		server.setProcessor(new FileStore.Processor<FileStore.Iface>(this.handler));
	}

	public void startServer() throws TTransportException {
		TServerTransport serverTransport = new TServerSocket(this.getPortNo());
		TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(this.processor));
		System.out.println("Server started : ");
		System.out.println("At nodeId: " + this.handler.nodeId.getId());
		System.out.println("IP Address: " + this.handler.ip);
		System.out.println("Port No: " + this.handler.port);
		server.serve();
	}
	public FileStoreHandler getHandler() {
		return handler;
	}

	public void setHandler(FileStoreHandler handler) {
		this.handler = handler;
	}

	public FileStore.Processor<FileStore.Iface> getProcessor() {
		return processor;
	}

	public void setProcessor(FileStore.Processor<FileStore.Iface> processor) {
		this.processor = processor;
	}

	public int getPortNo() {
		return portNo;
	}

	public void setPortNo(int portNo) {
		this.portNo = portNo;
	}

	public String getiPAddress() {
		return iPAddress;
	}

	public void setiPAddress(String iPAddress) {
		this.iPAddress = iPAddress;
	}
}
