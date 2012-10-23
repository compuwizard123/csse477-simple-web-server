/*****************************************************************************
 * $Id: DistributionAlgorithm.java,v 1.7 2003/08/02 05:32:26 jheiss Exp $
 *****************************************************************************
 * Base class for distribution algorithms:  algorithms for distributing
 * connections to the backend servers.
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

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;

public abstract class DistributionAlgorithm implements Runnable
{
	Distributor distributor;
	Logger logger;
	int connectionTimeout;
	TargetSelector targetSelector;
	Selector selector;
	// newClients needs to be accessible to subclasses, even if they are
	// in a different package, for use in proccessNewClients().
	protected List<SocketChannel> newClients;
	Map<SocketChannel, PendingConnectionState> pendingConnections;
	List<Connection> completedConnections;
	List<SocketChannel> failedConnections;
	TimedOutConnectionDetector timedOutDetector;
	Thread thread;
	int selectFailureOrZeroCount = 0;

	public DistributionAlgorithm(Distributor distributor)
	{
		this.distributor = distributor;

		// We can safely do this now instead of waiting for
		// finishInitialization() because we know it's one of the first
		// things Distributor does.  Some of our child constructors may
		// want to log things so we don't want to wait.
		logger = distributor.getLogger();
		//logger = Logger.getLogger(getClass().getName());

		newClients = new LinkedList<SocketChannel>();
		pendingConnections = new HashMap<SocketChannel, PendingConnectionState>();
		completedConnections = new LinkedList<Connection>();
		failedConnections = new LinkedList<SocketChannel>();

		try
		{
			selector = Selector.open();
		}
		catch (IOException e)
		{
			logger.severe("Error creating selector: " + e.getMessage());
			System.exit(1);
		}

		thread = new Thread(this, getClass().getName());
	}

	/*
 	* This allows Distributor to delay some of our initialization until
 	* it is ready.  There are some things we need that Distributor may
 	* not have ready at the point at which it constructs us.
 	*/
	public void finishInitialization()
	{
		connectionTimeout = distributor.getConnectionTimeout();
		targetSelector = distributor.getTargetSelector();
		timedOutDetector = new TimedOutConnectionDetector();
		thread.start();
	}

	/*
	 * TargetSelector uses this method to give us a client.
	 */
	void tryToConnect(SocketChannel client)
	{
		// Add the client to a queue which will be processed later
		// (in our thread instead of the caller's), by processNewClients().
		synchronized (newClients)
		{
			newClients.add(client);
		}

		// Wakeup the select so that the new client queue gets processed
		selector.wakeup();
	}

	/*
	 * Child classes must implement this method to process the
	 * newClients queue.  Implementations should empty the queue as they
	 * process it, i.e. via iter.remove().
	 *
	 * Should return true if it did something (i.e. the queue wasn't
	 * empty).
	 */
	protected abstract boolean processNewClients();

	/*
	 * Once an algorithm has picked a possible Target for a client, it
	 * uses this method to initiate a connection to that target.
	 *
	 * This method will generally be called from within
	 * processNewClients(); and possibly from within
	 * processFailedConnections(), depending on the distribution
	 * algorithm.
	 */
	public void initiateConnection(SocketChannel client, Target target)
	{
		try
		{
			SocketChannel connToServer = SocketChannel.open();
			connToServer.configureBlocking(false);

			// Initiate connection
			connToServer.connect(
				new InetSocketAddress(
					target.getInetAddress(),
					target.getPort()));

			// Use the client as the attachment to the key since
			// we'll need it later to lookup this connection's
			// state info in the pendingConnections map
			SelectionKey key = connToServer.register(
				selector, SelectionKey.OP_CONNECT, client);

			synchronized (pendingConnections)
			{
				// The target is needed later to create the
				//   Connection object if this connection succeeds
				// The time that the connection was initiated is
				//   used later in determining if this connection
				//   has timed out.
				// The selection key is needed so that it can be
				//   canceled if the connection does time out.
				pendingConnections.put(
					client,
					new PendingConnectionState(
						target, System.currentTimeMillis(), key));
			}
		}
		catch (IOException e)
		{
			logger.warning(
				"Error initiating connection to target: " +
				e.getMessage());
			synchronized(failedConnections)
			{
				failedConnections.add(client);
			}
		}
	}

	/*
	 * Select for connections which have completed, and call
	 * processFinishedConnections() with a list of those that have.
	 */
	public void run()
	{
		while (true)
		{
			boolean pncReturn = processNewClients();

			// Reset the failure counter if processNewClients() did
			// something, as that would explain why select would return
			// with zero ready channels.
			if (pncReturn) {
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

			try
			{
				int selectReturn = selector.select();

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
						" channels ready to connect");
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

			// Work through the list of channels that are ready
			Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
			while (keyIter.hasNext())
			{
				SelectionKey key = keyIter.next();
				keyIter.remove();

				SocketChannel server = (SocketChannel) key.channel();
				SocketChannel client = (SocketChannel) key.attachment();

				try
				{
					server.finishConnect();
					logger.fine(
						"Connection from " + client +
						" to " + server + " complete");
					synchronized(pendingConnections)
					{
						PendingConnectionState connState = pendingConnections.get(client);
						completedConnections.add(
							new Connection(
								client, server, connState.getTarget()));
						pendingConnections.remove(client);
					}
					key.cancel();
				}
				catch (IOException e)
				{
					logger.warning("Error finishing connection");
					key.cancel();
					try
					{
						server.close();
					}
					catch (IOException ioe)
					{
						logger.warning(
							"Error closing channel: " + ioe.getMessage());
					}

					synchronized(pendingConnections)
					{
						pendingConnections.remove(client);
					}
					synchronized(failedConnections)
					{
						failedConnections.add(client);
					}
				}
			}

			processCompletedConnections(completedConnections);
		}
	}

	/*
	 * Implementations should process the list of completed connections,
	 * generally by dumping any state they might have for that
	 * connection and then call targetSelector.addFinishedClient(conn).
	 * The list should be emptied as it is processed, i.e. via
	 * iter.remove().
	 */
	public abstract void processCompletedConnections(List<Connection> completedConnections);

	/*
	 * Implementations should process the list of failed connections,
	 * either by trying another target if that is appropriate or call
	 * targetSelector.addUnconnectedClient(client).
	 * The list should be emptied as it is processed, i.e. via
	 * iter.remove().
	 */
	public abstract void processFailedConnections(List<SocketChannel> failedConnections);

	/*
	 * Provide a default no-op implementation for this method since
	 * most algorithms don't care
	 */
	public void connectionNotify(Connection conn)
	{
		// no-op
	}

	/*
	 * Provide default no-op implementations for these methods since
	 * most algorithms won't need to do anything with the data
	 */
	public ByteBuffer reviewClientToServerData(
		SocketChannel client, SocketChannel server, ByteBuffer buffer)
	{
		return buffer;
	}
	public ByteBuffer reviewServerToClientData(
		SocketChannel server, SocketChannel client, ByteBuffer buffer)
	{
		return buffer;
	}

	class PendingConnectionState
	{
		Target target;
		long startTime;
		SelectionKey serverConnectionKey;

		PendingConnectionState(
			Target target,
			long startTime,
			SelectionKey serverConnectionKey)
		{
			this.target = target;
			this.startTime = startTime;
			this.serverConnectionKey = serverConnectionKey;
		}

		Target getTarget() { return target; }
		long getStartTime() { return startTime; }
		SelectionKey getServerKey() { return serverConnectionKey; }
	}

	public String getMemoryStats(String indent)
	{
		String stats = indent +
			newClients.size() + " entries in newClients List\n";
		stats += indent +
			pendingConnections.size() + " entries in pendingConnections Map\n";
		stats += indent +
			completedConnections.size() +
			" entries in completedConnections List\n";
		stats += indent +
			failedConnections.size() + " entries in failedConnections List\n";
		stats += indent +
			selector.keys().size() + " entries in selector key Set";

		return stats;
	}

	/*
	 * Look for connections which have timed out, add them to the list
	 * of connections which have failed for other reasons, and call
	 * processFailedConnections() with that list.
	 */
	class TimedOutConnectionDetector implements Runnable
	{
		TimedOutConnectionDetector()
		{
			Thread thread = new Thread(this, getClass().getName());
			thread.start();
		}

		public void run()
		{
			while (true)
			{
				// Add connections which have timed out to the list of
				// connections that have failed for other reasons.
				synchronized(pendingConnections)
				{
					Iterator<Entry<SocketChannel, PendingConnectionState>> iter = pendingConnections.entrySet().iterator();
					while(iter.hasNext())
					{
						Entry<SocketChannel, PendingConnectionState> pendingEntry = iter.next();
						SocketChannel client = pendingEntry.getKey();
						PendingConnectionState connState = pendingEntry.getValue();

						if (connState.getStartTime() + connectionTimeout < System.currentTimeMillis())
						{
							logger.finer("Pending connection from " + client + " to " + connState.getTarget() + " timed out");

							connState.getServerKey().cancel();
							iter.remove();

							// Add this client to the failed list
							synchronized(failedConnections)
							{
								failedConnections.add(client);
							}
						}
					}
				}

				// And get that list processed
				processFailedConnections(failedConnections);

				// Pause a reasonable amount of time before doing it
				// again
				try { Thread.sleep(connectionTimeout/2); }
					catch (InterruptedException e) {}
			}
		}
	}
}

