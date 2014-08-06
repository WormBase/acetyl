/*
 *                    BioJava development code
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  If you do not have a copy,
 * see:
 *
 *      http://www.gnu.org/copyleft/lesser.html
 *
 * Copyright for this code is held jointly by the individual
 * authors.  These should be listed in @author doc comments.
 *
 * For more information on the BioJava project and its aims,
 * or to join the biojava-l mailing list, visit the home page
 * at:
 *
 *      http://www.biojava.org/
 *
 */

package acetyl;

import java.util.*;
import java.io.*;
import java.net.*;
import java.security.*;

/**
 * Low level interface to the ACeDB sockets server.
 *
 * @author Thomas Down
 */

public class AceSocket {
    private final static int OK_MAGIC = 0x12345678;
    
    private final static String MSGREQ = "ACESERV_MSGREQ";
    private final static String MSGDATA = "ACESERV_MSGDATA";
    private final static String MSGOK = "ACESERV_MSGOK";
    private final static String MSGENCORE = "ACESERV_MSGENCORE";
    private final static String MSGFAIL = "ACESERV_MSGFAIL";
    private final static String MSGKILL = "ACESERV_MSGKILL";
    private Socket sock;
    private DataInputStream dis;
    private DataOutputStream dos;

    private boolean pendingConfig = true;
    private int serverVersion = 0;
    private int clientId = 0;
    boolean encore = false;
    private int maxBytes = 0;
    private boolean defunct = false;
    int transactionCount = 0;

    public AceSocket(String host, int port)
	throws Exception
    {
	this(host, port, "anonymous", "guest");
    }
    
    public AceSocket(String host, int port, String user, String passwd) 
	throws Exception 
    {
	try {
	    sock = new Socket(host, port);
	    dis = new DataInputStream(sock.getInputStream());
	    dos = new DataOutputStream(sock.getOutputStream());
	
	    String pad = transact("bonjour");
	    String userPasswd = md5Sum(user, passwd);
	    String token = md5Sum(userPasswd, pad);
	    String repl = transact(user + " " + token);
	    if (!repl.startsWith("et bonjour a vous"))
		throw new Exception("Couldn't connect ("+repl+")");
	} catch (IOException ex) {
	    throw new Exception(ex);
	}
    }

    public String transact(String s) throws Exception {
	++transactionCount;
	try {
	    if (dis.available() != 0) {
		handleUnsolicited();
	    }

	    writeMessage(MSGREQ, s);
	    String reply = readMessage();
	    return reply;
	} catch (IOException ex) {
	    throw new Exception(ex);
	}
    }

    public InputStream transactToStream(String s) throws Exception {
	++transactionCount;
	if (dis.available() != 0) {
	    handleUnsolicited();
	}

	writeMessage(MSGREQ, s);
	return new AceInputStream(this);
    }

    private void handleUnsolicited() throws Exception {
	defunct = true;
	throw new Exception("Unsolicited data from server!");
    }

    private void writeMessage(String type, String s) throws IOException {
	dos.writeInt(OK_MAGIC);
	dos.writeInt(s.length() + 1);
	dos.writeInt(serverVersion); // ???Server version???
	dos.writeInt(clientId); // clientId
	dos.writeInt(maxBytes); // maxBytes

	dos.writeBytes(type);

	byte[] padding = new byte[30 - type.length()];
	dos.write(padding, 0, padding.length);

	dos.writeBytes(s);
	dos.write(0);
	dos.flush();
    }

    private String readMessage() throws IOException {
	StringBuilder sb = new StringBuilder();
	while (readMessagePart(sb)) {
	    writeEncore();
	}
	return sb.toString();
    }

    void writeEncore()
	throws IOException
    {
	if (!encore)
	    throw new IllegalStateException();
	writeMessage(MSGENCORE, "encore");
    }
    
    private boolean readMessagePart(StringBuilder sb)
	throws IOException
    { 	
	sb.append(new String(read()));
	return this.encore;
    }

    byte[] read()
	throws IOException
    {
	int magic = dis.readInt();
	int length = dis.readInt();

	int rServerVersion = dis.readInt(); 
	int rClientId = dis.readInt();
	int rMaxBytes = dis.readInt();
	byte[] typeb = new byte[30];
	dis.readFully(typeb);
	String type = new String(typeb);
	
	if (pendingConfig) {
	    serverVersion = rServerVersion;
	    clientId = rClientId;
	    maxBytes = rMaxBytes;
	    pendingConfig = false;
	}

	byte[] message = new byte[length-1];
	dis.readFully(message);
	dis.skipBytes(1);
        

	this.encore = type.startsWith(MSGENCORE);
	return message;
    }
    
    public void dispose() throws Exception {
	try {
	    defunct = true;
	    sock.close();
	} catch (IOException ex) {
	    throw new Exception(ex);
	}
    }

    private static String md5Sum(String a, String b) {
	try {
	    MessageDigest md = MessageDigest.getInstance("MD5");

	    md.update(a.getBytes());
	    byte[] digest = md.digest(b.getBytes());
	    StringBuffer sb = new StringBuffer();
	    for (int i = 0; i < digest.length; ++i) {
		int bt = digest[i];
		sb.append(hexChar((bt >>> 4) & 0xf));
		sb.append(hexChar(bt & 0xf));
	    }
	    return sb.toString();
	} catch (NoSuchAlgorithmException ex) {
	    throw new Error("BioJava access to ACeDB sockets require the MD5 hash algorithm.  Consult your Java Vendor.");
	}
    }

    private static char hexChar(int i) {
	if (i <= 9)
	    return (char) ('0' + i);
	else
	    return (char) ('a' + i-10);
    }

    public boolean isDefunct() {
	return defunct;
    }
}

class AceInputStream  extends InputStream {
    private AceSocket socket;
    private int validity;
    
    private byte[] buffer;
    private int offset;

    AceInputStream(AceSocket s)
	throws IOException
    {
	this.socket = s;
	this.validity = this.socket.transactionCount;
	next();
    }

    private void validate()
	throws IOException
    {
	if (this.validity != this.socket.transactionCount)
	    throw new IOException("Tried to read from AceInputStream after a new transaction has been performed");
    }

    private boolean next()
	throws IOException
    {
	if (buffer == null || socket.encore) {
	    if (socket.encore)
		socket.writeEncore();
	    buffer = socket.read();
	    offset = 0;
	    return true;
	} else {
	    return false;
	}
    }
    
    public int read()
	throws IOException
    {
	validate();
	if (offset >= buffer.length)
	    if (!next())
		return -1;
	
	return buffer[offset++];
    }

    public int read(byte[] b)
	throws IOException
    {
	return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len)
	throws IOException
    {
	validate();
	if (offset >= buffer.length)
	    if (!next())
		return -1;

	int r = Math.min(len, buffer.length-offset);
	System.arraycopy(buffer, offset, b, off, r);
	offset += r;
	return r;
    }
}
