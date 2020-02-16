import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FileStoreHandler implements FileStore.Iface {

	public int port;
	public String ip;
	public NodeID nodeId;
	public Map<String, RFile> metadata;
	public List<NodeID> fingerTable;
	
	public FileStoreHandler(String ip, int port) {
		this.port = port;
		this.ip = ip;
		this.nodeId = new NodeID(getSHA256ForKey(ip + ":" + port), ip, port);
		this.metadata = new LinkedHashMap<String, RFile>();
	}

	@Override
	public void writeFile(RFile rFile) throws SystemException, TException {
		String fName = rFile.getMeta().getFilename();
		File file = new File(fName);
		String sha_hash = getSHA256ForKey(fName.trim());
		NodeID nodeId = findSucc(sha_hash);
		checkIfServerOwnsFile(nodeId);
		try 
		{
			rFile = checkIfFileExists(rFile, file, rFile.getContent(), rFile.getMeta());
			metadata.put(fName, rFile);
			FileWriter wr = new FileWriter(file, false);
			BufferedWriter bwr = new BufferedWriter(wr);
			bwr.write(rFile.getContent());
			bwr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * If the filename does not exist on the server, a new file should be created with its version attribute set to 0.
Otherwise, the file contents should be overwritten, and the version number should be incremented.
	 * @param rFile
	 * @param file
	 * @param fContent
	 * @param meta
	 * @return
	 */
	private RFile checkIfFileExists(RFile rFile, File file, String fContent, RFileMetadata meta) {
		String contentHash;
		if (!this.metadata.containsKey(file.getName())) {
			rFile.setContent(fContent);
			meta.setVersion(0);
			if (fContent != null) {
				contentHash = getSHA256ForKey(fContent.trim());
				meta.setContentHash(contentHash);
			}
			rFile.setMeta(meta);

		} else {
			RFile extFile = this.metadata.get(file.getName());
			RFileMetadata existingMeta = extFile.getMeta();
			existingMeta.setVersion(existingMeta.getVersion() + 1);
			extFile.setContent(rFile.getContent());
			contentHash = getSHA256ForKey(fContent.trim());
			existingMeta.setContentHash(contentHash);
			extFile.setMeta(existingMeta);
			rFile = extFile;
		}
		return rFile;
	}

	/**
	 * If the server does not own the files id, i.e., the server is not the files
	 * successor, a SystemException should be thrown
	 * 
	 * @param nodeId
	 * @throws SystemException
	 */
	private void checkIfServerOwnsFile(NodeID nodeId) throws SystemException {
		if (!nodeId.equals(this.nodeId)) {
			System.err.println("Server doesn't owns the file");
			throw new SystemException().setMessage("Server doesn't owns the file");
		}
	}

	/**
	 *if a file with a given name exists on the server, both the contents and meta-information should be returned.
	 * @param fname
	 * @return rFile
	 *
	 */
	@Override
	public RFile readFile(String fname) throws SystemException, TException {
		String fHash = getSHA256ForKey(fname);
		NodeID node = findSucc(fHash);
		if (node.equals(nodeId)) {
			File file = new File(fname);
			if (!metadata.containsKey(file.getName())) {
				System.err.println("File doesn't exists on server.");
				throw new SystemException().setMessage("Error in reading File : File doesn't exists on server.");
			}
			RFile existingFile = metadata.get(fname);

			return existingFile;
		}
		else {
			System.err.println("File doesn't exists on server., i.e., the server is not the files successor,");
			throw new SystemException().setMessage("File doesn't exists on server, i.e. server is not the files successor,");
		}
	}

	@Override
	public void setFingertable(List<NodeID> nodes) throws TException {
		this.fingerTable = nodes;
	}

	/**
	 *
	 * @param key
	 * @return NodeID
	 *
	 */

	@Override
	public NodeID findSucc(String key) throws SystemException, TException {
		NodeID predecessor = findPred(key);
		String preIp = "";
		int prePort = 0;

		if (predecessor != null) {
			preIp = predecessor.getIp();
			prePort = predecessor.getPort();
		}
		TTransport transport = new TSocket(preIp, prePort);
		transport.open();
		FileStore.Client client = new FileStore.Client(new TBinaryProtocol(transport));
		if (client != null) {
			return client.getNodeSucc();
		}
		return null;
	}

	/**
	 *
	 * @param key
	 * @return NodeID
	 */
	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		if (fingerTable == null || fingerTable.isEmpty()) 
			throw new SystemException()
					.setMessage("Error : FingerTable doesn't exists for current node.");
		if (!isBetweenRange(key, nodeId.getId(), fingerTable.get(0).getId())) {
			for (int i = fingerTable.size() - 1; i > 0; i--) {
				if (isBetweenRange(fingerTable.get(i).getId(), nodeId.getId(), key)) {
					return findPredRec(key, fingerTable.get(i));
				}
			}
		}
		return nodeId;
	}

	private boolean isBetweenRange(String key, String nodeId, String successorNodeId) {
		if (nodeId.compareTo(successorNodeId) < 0) {
			if ((key.compareTo(nodeId) > 0 && key.compareTo(successorNodeId) <= 0))
				return true;
			else
				return false;
		}
		else if (((key.compareTo(nodeId) > 0 && key.compareTo(successorNodeId) >= 0) || (key.compareTo(nodeId) < 0 && key.compareTo(successorNodeId) <= 0)))
			return true;
		else
			return false;

	}

	/**
	 *
	 * @param key
	 * @param nodeId
	 * @return
	 */
	public NodeID findPredRec(String key, NodeID nodeId) {

		TTransport tTransport = new TSocket(nodeId.getIp(), nodeId.getPort());
		try {
			tTransport.open();
			TProtocol protocol = new TBinaryProtocol(tTransport);
			FileStore.Client client = new FileStore.Client(protocol);
			return client.findPred(key);

		} catch (SystemException e) {
			System.err.println(e.getMessage());
		} catch (TTransportException e) {
			System.err.println("Error in TTransport socket connection : " + e.getMessage());
		} catch (TException e) {
			e.printStackTrace();
		}
		return nodeId;
	}

	/**
	 * returns the closest DHT node that follows the current node in the Chord key
	 * space, i.e., the first entry in the nodes fingertable. A SystemException
	 * should be thrown if no fingertable exists for the current node.
	 * 
	 * @param
	 * @return NodeID
	 */
	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		if (fingerTable != null && !fingerTable.isEmpty())
			return fingerTable.get(0);
		else {
			System.err.println("Fingertable does not exists for current node. Execute the init file.");
			System.err.println("Execute the init file");
			throw new SystemException().setMessage("Fingertable does not exists for current node");
		}
	}

	/**
	 * Converts a key to SHA-256 format
	 * 
	 * @param key
	 * @return
	 */
	public String getSHA256ForKey(String key) {
		StringBuilder sha = new StringBuilder();
		if (key != null && key.length() > 0) {
			MessageDigest digest = null;
			try {
				digest = MessageDigest.getInstance("SHA-256");
			} catch (NoSuchAlgorithmException e) {
				System.err.println("Error occured in converting a key to SHA-256");
				e.printStackTrace();
				System.exit(0);
			}
			digest.update(key.trim().getBytes());
			for (byte b : digest.digest())
				sha.append(String.format("%02x", b));
		}
		return sha.toString();
	}
}
