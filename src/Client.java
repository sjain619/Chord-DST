import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {

	public String iPAddress;
	public int portNo;
	public String fileName;
	public String action;

	public String getiPAddress() {
		return iPAddress;
	}

	public void setiPAddress(String iPAddress) {
		this.iPAddress = iPAddress;
	}

	public int getPortNo() {
		return portNo;
	}

	public void setPortNo(int portNo) {
		this.portNo = portNo;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}
	public static void main(String[] args) {

		try {
			Client c = new Client();
			c.setiPAddress(args[0]);
			c.setPortNo(Integer.parseInt(args[1]));
			c.setFileName(args[2]);
			c.setFileName(args[3]);
			TTransport transport = new TSocket(c.getiPAddress(), c.getPortNo());
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			if (c.action.equals("read")) {
				c.readFileInClient(client, c.getFileName());
			} else if (c.action.equals("write")) {
				c.writeFileInClient(client, c.getiPAddress(), c.getPortNo(), c.getFileName());
			}
			transport.close();
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Incorrect input, please follow this format: ./client.sh :iPAddress :portNo :filename :read/write");
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
		}
	}

	private void writeFileInClient(FileStore.Client client, String ip, int port, String filename) {
		RFile file = new RFile();
		RFileMetadata metadata = new RFileMetadata();

		metadata.setFilename(this.getFileName());
		metadata.setFilenameIsSet(true);
		try {
			String content = new String(Files.readAllBytes(Paths.get(this.getFileName())));
			metadata.setContentHash(getSHA256ForKey(content.trim()));
			metadata.setContentHashIsSet(true);
			file.setMeta(metadata);
			file.setContent(content);
			file.setContentIsSet(true);
			NodeID node = client.findSucc(getSHA256ForKey(this.getFileName()));
			System.out.println("Destination Node :: " + node.toString());
			client.writeFile(file);

		} catch (IOException ex) {
			System.err.println("Error occurred File does not exist. " + ex.getMessage());
			System.exit(0);
		} catch (SystemException ex) {
			System.err.println("SystemException in writing file : " + ex.getMessage());
			System.exit(0);
		} catch (TException ex) {
			System.err.println("Error: Fingertable is not set. " + ex.getMessage());
			System.exit(0);
		}

	}

	private void readFileInClient(FileStore.Client client, String filename) {
		try {
			RFile rFile = client.readFile(this.getFileName());

			if (rFile != null) {
				System.out.println("Successfully read the file.");
				System.out.println("Filename : " + rFile.getMeta().getFilename());
				System.out.println("Version : " + rFile.getMeta().getVersion());
				System.out.println("Content Hash : " + rFile.getMeta().getContentHash());
				System.out.println("Content : " + rFile.getContent());
			}
		} catch (SystemException e) {
			System.err.println("Exception occured while reading file : " + e.getMessage());
			System.exit(0);
		} catch (TException ex) {
			System.err.println("Error in readFileInClient, Fingertable is not set. " + ex.getMessage());
			System.exit(0);
		}

	}

	public String getSHA256ForKey(String key) {
		StringBuilder sha = new StringBuilder();
		if (key != null && key.length() > 0) {
			try {
				MessageDigest digest = MessageDigest.getInstance("SHA-256");
				digest.update(key.trim().getBytes());
				byte[] bts = digest.digest();
				for (int i=0; i<bts.length; i++)
					sha.append(String.format("%02x", bts[i]));
			} catch (NoSuchAlgorithmException e) {
				System.err.println("Exception occured while converting a key to SHA-256");
				System.exit(0);
			}
		}
		return sha.toString();
	}
}
