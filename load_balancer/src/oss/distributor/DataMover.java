/*
 *****************************************************************************
 * $Id: DataMover.java,v 1.14 2003/08/06 21:26:13 jheiss Exp $
 *****************************************************************************
 * This class passes data back and forth from clients and servers for
 * established connections through the load balancer.
 *****************************************************************************
 * Copyright 2003 Jason Heiss
 * 
 * This file is part of Distributor.
 * 
 * Distributor is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * Distributor is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Distributor; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *****************************************************************************
 */

package oss.distributor;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CancelledKeyException;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

class DataMover implements Runnable
{
	Target target;
	boolean halfClose;
	Selector selector;
	Logger logger;
	List<DistributionAlgorithm> distributionAlgorithms;
	Map<SocketChannel, SocketChannel> clients;
	Map<SocketChannel, SocketChannel> servers;
	List<Connection> newConnections;
	List<SocketChannel> channelsToReactivate;
	DelayedMover delayedMover;
	long clientToServerByteCount;
	long serverToClientByteCount;
	Thread thread;

	final int BUFFER_SIZE = 128 * 1024;

	protected DataMover(
		Distributor distributor, Target target, boolean halfClose)
	{
		logger = distributor.getLogger();
		distributionAlgorithms = distributor.getDistributionAlgorithms();
		this.target = target;
		this.halfClose = halfClose;

		try
		{
			selector = Selector.open();
		}
		catch (IOException e)
		{
			logger.severe("Error creating selector: " + e.getMessage());
			System.exit(1);
		}

		clients = new HashMap<SocketChannel, SocketChannel>();
		servers = new HashMap<SocketChannel, SocketChannel>();
		newConnections = new LinkedList<Connection>();
		channelsToReactivate = new LinkedList<SocketChannel>();

		delayedMover = new DelayedMover();

		clientToServerByteCount = 0;
		serverToClientByteCount = 0;

		// Create a thread for ourselves and start it
		thread = new Thread(this, toString());
		thread.start();
	}

	/*
	 * Completed connections established by a distribution algorithm are
	 * handed to the corresponding Target, which it turn registers them
	 * with us via this method.
	 */
	protected void addConnection(Connection conn)
	{
		// Add connection to a list that will be processed later by
		// calling processNewConnections()
		synchronized (newConnections)
		{
			newConnections.add(conn);
		}

		// Wakeup the select so that the new connection list gets
		// processed
		selector.wakeup();
	}

	/*
	 * Process new connections queued up by calls to addConnection()
	 *
	 * Returns true if it did something (i.e. the queue wasn't empty).
	 */
	private boolean processNewConnections()
	{
		boolean didSomething = false;

		synchronized (newConnections)
		{
			Iterator<Connection> iter = newConnections.iterator();
			while(iter.hasNext())
			{
				Connection conn = iter.next();
				iter.remove();

				SocketChannel client = conn.getClient();
				SocketChannel server = conn.getServer();

				try
				{
					logger.finest("Setting channels to non-blocking mode");
					client.configureBlocking(false);
					server.configureBlocking(false);

					clients.put(client, server);
					servers.put(server, client);

					logger.finest("Registering channels with selector");
					client.register(selector, SelectionKey.OP_READ);
					server.register(selector, SelectionKey.OP_READ);
				}
				catch (IOException e)
				{
					logger.warning(
						"Error setting channels to non-blocking mode: " +
						e.getMessage());
					try
					{
						logger.fine("Closing channels");
						client.close();
						server.close();
					}
					catch (IOException ioe)
					{
						logger.warning("Error closing channels: " +
							ioe.getMessage());
					}
				}

				didSomething = true;
			}
		}

		return didSomething;
	}

	/*
	 * In the moveData() method, if we have a destination channel which
	 * we aren't immediately able to write data to, we de-activate the
	 * corresponding source channel from the selector until DelayedMover
	 * is able to transmit all of that delayed data.  This method is
	 * used by DelayedMover to tell us that all of the data from a
	 * channel has been sent to its destination, and thus that we can
	 * re-activate the channel with the selector and read more data from
	 * it.
	 */
	protected void addToReactivateList(SocketChannel channel)
	{
		// Add channel to a list that will be processed later by
		// calling processReactivateList()
		synchronized (channelsToReactivate)
		{
			channelsToReactivate.add(channel);
		}

		// Wakeup the select so that the list gets processed
		selector.wakeup();
	}

	/*
	 * Process channels queued up by calls to addToReactivateList()
	 *
	 * Returns true if it did something (i.e. the queue wasn't empty).
	 */
	private boolean processReactivateList()
	{
		boolean didSomething = false;

		synchronized (channelsToReactivate)
		{
			Iterator<SocketChannel> iter = channelsToReactivate.iterator();
			while(iter.hasNext())
			{
				SocketChannel channel = iter.next();
				iter.remove();

				SelectionKey key = channel.keyFor(selector);

				try
				{
					// Add OP_READ back to the interest bits
					key.interestOps(
						key.interestOps() | SelectionKey.OP_READ);
				}
				catch (CancelledKeyException e)
				{
					// The channel has been closed or something similar,
					// nothing we can do about it.
				}

				didSomething = true;
			}
		}

		return didSomething;
	}

	public void run()
	{
		int selectFailureOrZeroCount = 0;

		ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

		while(true)
		{
			// Register any new connections with the selector
			boolean pncReturn = processNewConnections();
			// Re-activate channels with the selector
			boolean prlReturn = processReactivateList();

			// Reset the failure counter if processNewConnections() or
			// processReactivateList() did something, as that would
			// explain why select would return with zero ready channels.
			if (pncReturn || prlReturn)
			{
				selectFailureOrZeroCount = 0;
			}

			// If we exceed the threshold of failed selects, pause
			// for a bit so we don't go into a tight loop
			if (selectFailureOrZeroCount >= 10)
			{
				logger.warning(
					"select appears to be failing repeatedly, pausing");
				try { Thread.sleep(500); }
					catch (InterruptedException e) {}
				selectFailureOrZeroCount = 0;
			}

			//
			// Now select for any channels that have data to be moved
			//
			int selectReturn = 0;
			try
			{
				selectReturn = selector.select();

				if (selectReturn > 0)
				{
					selectFailureOrZeroCount = 0;
				}
				else
				{
					selectFailureOrZeroCount++;
				}
			}
			catch (IOException e)
			{
				// The only exceptions thrown by select seem to be the
				// occasional (fairly rare) "Interrupted system call"
				// which, from what I can tell, is safe to ignore.
				logger.warning(
					"Error when selecting for ready channel: " +
					e.getMessage());
				selectFailureOrZeroCount++;
				continue;
			}

			logger.finest(
				"select reports " + selectReturn + " channels ready to read");

			// Work through the list of channels that have data to read
			Iterator<SelectionKey>  keyIter = selector.selectedKeys().iterator();
			while (keyIter.hasNext())
			{
				SelectionKey key = keyIter.next();
				keyIter.remove();

				// Figure out which direction this data is going and
				// get the SocketChannel that is the other half of
				// the connection.
				SocketChannel src = (SocketChannel) key.channel();
				SocketChannel dst;
				boolean clientToServer;
				if (clients.containsKey(src))
				{
					clientToServer = true;
					dst = clients.get(src);
				}
				else if (servers.containsKey(src))
				{
					clientToServer = false;
					dst = servers.get(src);
				}
				else
				{
					// We've been dropped from the maps, which means the
					// connection has already been closed.  Nothing to
					// do except cancel our key (just to be safe) and
					// move on to the next ready key.
					key.cancel();
					continue;
				}

				try
				{
					// Loop as long as the source has data to read
					// and we can write it to the destination.
					boolean readMore = true;
					while (readMore) {
						// Assume there won't be more data
						readMore = false;

						// Try to read data
						buffer.clear();
						int numberOfBytes = src.read(buffer);
						logger.finest(
							"Read " + numberOfBytes + " bytes from " + src);

						if (numberOfBytes > 0)  // Data was read
						{
							if (moveData(
								buffer, src, dst, clientToServer, key))
							{
								readMore = true;
							}
						}
						else if (numberOfBytes == -1)  // EOF
						{
							handleEOF(key, src, dst, clientToServer);
						}
					}
				}
				catch (IOException e)
				{
					logger.warning(
						"Error moving data between channels: " +
						e.getMessage());
					closeConnection(src, dst, clientToServer);
				}
			}
		}
	}

	/*
	 * Give the distribution algorithms a chance to review the data in
	 * buffer, then attempt to send it to dst.
	 *
	 * Returns true is all of the data in buffer is successfully
	 * transmitted to dst, false if some/all of it is delayed.
	 */
	private boolean moveData(
		ByteBuffer buffer,
		SocketChannel src, SocketChannel dst,
		boolean clientToServer, SelectionKey sourceKey) throws IOException
	{
		buffer.flip();

		if (clientToServer)
		{
			clientToServerByteCount += buffer.remaining();
		}
		else
		{
			serverToClientByteCount += buffer.remaining();
		}

		// Give each of the distribution algorithms a
		// chance to inspect/modify the data stream
		Iterator<DistributionAlgorithm> iter = distributionAlgorithms.iterator();
		ByteBuffer reviewedBuffer = buffer;
		while (iter.hasNext())
		{
			DistributionAlgorithm algo = iter.next();
			if (clientToServer)
			{
				reviewedBuffer =
					algo.reviewClientToServerData(src, dst, reviewedBuffer);
			}
			else
			{
				reviewedBuffer =
					algo.reviewServerToClientData(src, dst, reviewedBuffer);
			}
		}

		// Make an effort to send the data on to its destination
		dst.write(reviewedBuffer);

		// If there is still data in the buffer, hand it off to
		// DelayedMover
		if (reviewedBuffer.hasRemaining())
		{
			logger.finer("Delaying " + reviewedBuffer.remaining() +
				" bytes from " + src + " to " + dst);

			// Copy the delayed data into a temporary buffer
			ByteBuffer delayedBuffer =
				ByteBuffer.allocate(reviewedBuffer.remaining());
			delayedBuffer.put(reviewedBuffer);
			delayedBuffer.flip();

			// De-activate the source channel from the selector by
			// removing OP_READ from the interest bits.  (This is safer
			// than actually canceling the key and then re-registering
			// the channel later, there are race condition problems with
			// that approach leading to CanceledKeyExceptions.)  We
			// don't want to read any more data from the source until we
			// get this delayed data written to the destination.
			// DelayedMover will re-activate the source channel (via
			// addToReactivateList()) when it has written all of the
			// delayed data.
			try
			{
				sourceKey.interestOps(
					sourceKey.interestOps() ^ SelectionKey.OP_READ);

				delayedMover.addToQueue(
					new DelayedDataInfo(
						dst, delayedBuffer, src, clientToServer));
			}
			catch (CancelledKeyException e)
			{
				// The channel has been closed or something similar,
				// nothing we can do about it.
			}

			return false;
		}
		else
		{
			return true;
		}
	}

	private void handleEOF(
		SelectionKey key,
		SocketChannel src, SocketChannel dst,
		boolean clientToServer) throws IOException
	{
		if (halfClose)
		{
			// Cancel this key, otherwise this channel will repeatedly
			// trigger select to tell us that it is at EOF.
			key.cancel();

			Socket srcSocket = src.socket();
			Socket dstSocket = dst.socket();

			// If the other half of the socket is already shutdown then
			// go ahead and close the socket
			if (srcSocket.isOutputShutdown())
			{
				logger.finer("Closing source socket");
				srcSocket.close();
			}
			// Otherwise just close down the input stream.  This allows
			// any return traffic to continue to flow.
			else
			{
				logger.finest("Shutting down source input");
				srcSocket.shutdownInput();
			}

			// Do the same thing for the destination, but using the
			// reverse streams.
			if (dstSocket.isInputShutdown())
			{
				logger.finer("Closing destination socket");
				dstSocket.close();
			}
			else
			{
				logger.finest("Shutting down dest output");
				dstSocket.shutdownOutput();
			}

			// Clean up if both halves of the connection are now closed
			if (srcSocket.isClosed() && dstSocket.isClosed())
			{
				dumpState(src, dst, clientToServer);
			}
		}
		else
		{
			// If half close isn't enabled, just close the connection.
			closeConnection(src, dst, clientToServer);
		}
	}

	private void closeConnection(
		SocketChannel src, SocketChannel dst, boolean clientToServer)
	{
		SocketChannel client;
		SocketChannel server;

		if (clientToServer)
		{
			client = src;
			server = dst;
		}
		else
		{
			server = src;
			client = dst;
		}

		closeConnection(client, server);
	}

	protected void closeConnection(
		SocketChannel client, SocketChannel server)
	{
		// Close both channels
		try
		{
			logger.fine("Closing channels");
			client.close();
			server.close();
		}
		catch (IOException ioe)
		{
			logger.warning("Error closing channels: " + ioe.getMessage());
		}

		dumpState(client, server);
	}

	private void dumpState(
		SocketChannel src, SocketChannel dst, boolean clientToServer)
	{
		SocketChannel client;
		SocketChannel server;

		if (clientToServer)
		{
			client = src;
			server = dst;
		}
		else
		{
			server = src;
			client = dst;
		}

		dumpState(client, server);
	}

	/*
	 * Call this method when closing a connection to remove any
	 * associated entries from the state tracking maps.
	 */
	private void dumpState(SocketChannel client, SocketChannel server)
	{
		clients.remove(client);
		servers.remove(server);

		delayedMover.dumpDelayedState(client, server);
	}

	public long getClientToServerByteCount()
	{
		return clientToServerByteCount;
	}

	public long getServerToClientByteCount()
	{
		return serverToClientByteCount;
	}

	public String toString()
	{
		return getClass().getName() +
			" for " + target.getInetAddress() + ":" + target.getPort();
	}

	protected String getMemoryStats(String indent)
	{
		String stats = indent + clients.size() + " entries in clients Map\n";
		stats += indent + servers.size() + " entries in servers Map\n";
		stats += indent +
			newConnections.size() + " entries in newConnections List\n";
		stats += indent +
			channelsToReactivate.size() +
			" entries in channelsToReactivate List\n";
		stats += indent +
			selector.keys().size() + " entries in selector key Set\n";
		stats += indent + "DelayedMover:\n";
		stats += delayedMover.getMemoryStats(indent);

		return stats;
	}

	class DelayedMover implements Runnable
	{
		Selector delayedSelector;
		List<DelayedDataInfo> queue;
		Map<SocketChannel, DelayedDataInfo> delayedInfo;
		Thread thread;

		DelayedMover()
		{
			try
			{
				delayedSelector = Selector.open();
			}
			catch (IOException e)
			{
				logger.severe("Error creating selector: " + e.getMessage());
				System.exit(1);
			}

			queue = new LinkedList<DelayedDataInfo>();
			delayedInfo = new HashMap<SocketChannel, DelayedDataInfo>();

			// Create a thread for ourselves and start it
			thread = new Thread(this, toString());
			thread.start();
		}

		/*
		 * Used by DataMover to register a destination with us.
		 */
		void addToQueue(DelayedDataInfo info)
		{
			// Add channel to a list that will be processed later by
			// calling processQueue()
			synchronized (queue)
			{
				queue.add(info);
			}

			// Wakeup the select so that the new connection list
			// gets processed
			delayedSelector.wakeup();
		}

		/*
		 * Process the list created by addToQueue()
		 */
		private boolean processQueue()
		{
			boolean didSomething = false;

			synchronized (queue)
			{
				Iterator<DelayedDataInfo> iter = queue.iterator();
				while (iter.hasNext())
				{
					DelayedDataInfo info = iter.next();
					iter.remove();

					SocketChannel dst = info.getDest();

					// Store the info in a map for later use
					synchronized (delayedInfo)
					{
						delayedInfo.put(dst, info);
					}

					// Check to see if we already have a key registered
					// for this channel.
					SelectionKey key = dst.keyFor(delayedSelector);
					if (key == null)
					{
						// Nope, no key already registered.  Register a
						// new one.
						logger.finest(
							"Registering channel with selector");
						try
						{
							dst.register(
								delayedSelector, SelectionKey.OP_WRITE);
						}
						catch (ClosedChannelException e)
						{
							// If the channel is already closed, there isn't
							// much else we can do to it.  DataMover will
							// clean things up.
						}
					}
					else
					{
						// We already have a key registered, make sure
						// it has the right interest bits.
						try
						{
							key.interestOps(
								key.interestOps() | SelectionKey.OP_WRITE);
						}
						catch (CancelledKeyException e)
						{
							// The channel has been closed or something
							// similar, nothing we can do about it.
							// DataMover will clean things up.
						}
					}

					didSomething = true;
				}
			}

			return didSomething;
		}

		public void run()
		{
			int selectFailureOrZeroCount = 0;

			while (true)
			{
				// Register any new connections with the selector
				boolean pqReturn = processQueue();

				// Reset the failure counter if processQueue() did
				// something, as that would explain why select would
				// return with zero ready channels.
				if (pqReturn)
				{
					selectFailureOrZeroCount = 0;
				}

				// If we exceed the threshold of failed selects, pause
				// for a bit so we don't go into a tight loop
				if (selectFailureOrZeroCount >= 10)
				{
					logger.warning(
						"select appears to be failing repeatedly, pausing");
					try { Thread.sleep(500); }
						catch (InterruptedException e) {}
					selectFailureOrZeroCount = 0;
				}

				// Now select for any channels that are ready to write
				try
				{
					int selectReturn = delayedSelector.select();

					if (selectReturn > 0)
					{
						selectFailureOrZeroCount = 0;
					}
					else
					{
						selectFailureOrZeroCount++;
					}
					logger.finest(
							"select reports " + selectReturn +
							" channels ready to write");
				}
				catch (IOException e)
				{
					// The only exceptions thrown by select seem to be the
					// occasional (fairly rare) "Interrupted system call"
					// which, from what I can tell, is safe to ignore.
					logger.warning(
						"Error when selecting for ready channel: " +
						e.getMessage());
					selectFailureOrZeroCount++;
					continue;
				}

				// Work through the list of channels that are
				// ready to write
				Iterator<SelectionKey> keyIter = delayedSelector.selectedKeys().iterator();
				SelectionKey key = keyIter.next();
				keyIter.remove();

				SocketChannel dst = (SocketChannel) key.channel();
				DelayedDataInfo info;
				synchronized (delayedInfo)
				{
					info = delayedInfo.get(dst);
				}
				ByteBuffer delayedBuffer = info.getBuffer();

				try
				{
					int numberOfBytes = dst.write(delayedBuffer);

					logger.finest(
						"Wrote " + numberOfBytes +
						" delayed bytes to " + dst + ", " +
						delayedBuffer.remaining() +
						" bytes remain delayed");

					// If the buffer is now empty, we're done with
					// this channel.
					if (! delayedBuffer.hasRemaining())
					{
						// Instead of canceling the key, we just
						// remove OP_WRITE from the interest bits.
						// This avoids a race condition leading to a
						// CanceledKeyException if DataMover gives
						// the channel back to us right away.  It
						// means we're stuck with the key in our
						// selector until the connection is closed,
						// but that seems acceptable.
						try
						{
							key.interestOps(
								key.interestOps() ^ SelectionKey.OP_WRITE);
						}
						catch (CancelledKeyException e)
						{
							// The channel has been closed or something
							// similar, nothing we can do about it.
							// DataMover will clean things up.
						}

						SocketChannel src = info.getSource();
						dumpDelayedState(info.getDest());
						addToReactivateList(src);
					}
				}
				catch (IOException e)
				{
					logger.warning(
						"Error writing delayed data: " +
						e.getMessage());
					closeConnection(
						dst, info.getSource(), info.isClientToServer());
				}
			}
		}

		void dumpDelayedState(SocketChannel client, SocketChannel server)
		{
			dumpDelayedState(client);
			dumpDelayedState(server);
		}

		private void dumpDelayedState(SocketChannel dst)
		{
			synchronized (delayedInfo)
			{
				delayedInfo.remove(dst);
			}
		}

		public String toString()
		{
			return getClass().getName() +
				" for " + target.getInetAddress() + ":" + target.getPort();
		}

		protected String getMemoryStats(String indent)
		{
			String stats = indent + queue.size() + " entries in queue List\n";
			stats = indent +
				delayedInfo.size() + " entries in delayedInfo Map\n";
			stats += indent +
				delayedSelector.keys().size() +
				" entries in delayedSelector key Set";

			return stats;
		}
	}

	class DelayedDataInfo
	{
		SocketChannel dst;
		ByteBuffer buffer;
		SocketChannel src;
		boolean clientToServer;

		DelayedDataInfo(
			SocketChannel dst, ByteBuffer buffer,
			SocketChannel src, boolean clientToServer)
		{
			this.dst = dst;
			this.buffer = buffer;
			this.src = src;
			this.clientToServer = clientToServer;
		}

		SocketChannel getDest() { return dst; }
		ByteBuffer getBuffer() { return buffer; }
		SocketChannel getSource() { return src; }
		boolean isClientToServer() { return clientToServer; }
	}
}

