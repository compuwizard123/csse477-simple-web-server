/*
 *****************************************************************************
 * $Id: TargetSelector.java,v 1.5 2003/06/21 19:03:56 jheiss Exp $
 *****************************************************************************
 * Marshall new connections through the process of having distribution
 * algorithms pick a target for them, and then hand the completed
 * connections to the targets to perform the bulk data transfer for the
 * life of the connection.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.io.IOException;
import java.nio.channels.SocketChannel;

public class TargetSelector implements Runnable
{
	Distributor distributor;
	Logger logger;
	List<DistributionAlgorithm> distributionAlgorithms;
	Map<SocketChannel, DistributionAlgorithm> currentAlgorithm;
	List<SocketChannel> needsDistributing;
	List<Connection> finishedDistributing;
	Thread thread;

	protected TargetSelector(Distributor distributor)
	{
		this.distributor = distributor;

		currentAlgorithm = new HashMap<SocketChannel, DistributionAlgorithm>();
		needsDistributing = new LinkedList<SocketChannel>();
		finishedDistributing = new LinkedList<Connection>();

		thread = new Thread(this, getClass().getName());
	}

	/*
	 * This allows Distributor to delay some of our initialization until
	 * it is ready.  There are some things we need that Distributor may
	 * not have ready at the point at which it constructs us.
	 */
	protected void finishInitialization()
	{
		logger = distributor.getLogger();
		distributionAlgorithms = distributor.getDistributionAlgorithms();
		thread.start();
	}

	/*
	 * Used by Distributor to give us a new client
	 */
	protected void addNewClient(SocketChannel client)
	{
		try
		{
			logger.finest("Setting client channel to non-blocking mode");
			client.configureBlocking(false);
			addUnconnectedClient(client);
		}
		catch (IOException e)
		{
			logger.warning(
				"Error setting channels to non-blocking mode: " +
				e.getMessage());
			try
			{
				logger.fine("Closing client channel");
				client.close();
			}
			catch (IOException ioe)
			{
				logger.warning(
					"Error closing client channel: " + ioe.getMessage());
			}
		}
	}

	/*
	 * Used by addNewClient(), or by a DistributionAlgorithm to give us
	 * a client which that algorithm was not able to connect to a
	 * Target.
	 *
	 * See the note near the wait() call in run() as to why this method
	 * is synchronized.
	 */
	public synchronized void addUnconnectedClient(SocketChannel client)
	{
		synchronized (needsDistributing)
		{
			needsDistributing.add(client);
		}

		// Wake up this class' thread so it can process the client
		notify();
	}

	/*
	 * Used by a DistributionAlgorithm to give us a completed
	 * connection.
	 *
	 * See the note near the wait() call in run() as to why this method
	 * is synchronized.
	 */
	public synchronized void addFinishedClient(Connection conn)
	{
		synchronized (finishedDistributing)
		{
			finishedDistributing.add(conn);
		}

		// Wake up this class' thread so it can process the client
		notify();
	}

	public void run()
	{
		while (true)
		{
			// If the queues are empty, give up our synchronization lock
			// on 'this' and wait for addUnconnectedClient or
			// addFinishedClient to notify us that there is a client to
			// process.  Synchronization on 'this' is to prevent the
			// addUnconnectedClient() and addFinishedClient() methods
			// from sneaking another entry into the queue while we're
			// checking the sizes.
			synchronized (this)
			{
				if (needsDistributing.size() == 0 &&
					finishedDistributing.size() == 0)
				{
					try { wait(); } catch (InterruptedException e) {}
				}
			}

			//
			// Handle clients which need to be distributed
			//

			synchronized (needsDistributing)
			{
				Iterator<SocketChannel> iter = needsDistributing.iterator();
				while (iter.hasNext())
				{
					SocketChannel client = iter.next();
					iter.remove();

					//
					// Figure out which algorithm to use for this client
					//

					// Get the last algorithm used
					DistributionAlgorithm algo = currentAlgorithm.get(client);

					// New clients won't be in the map and thus we'll
					// get null.  Start them off with the first algorithm.
					if (algo == null)
					{
						algo = distributionAlgorithms.get(0);
					}
					// Otherwise advance to the next algorithm
					else
					{
						int i = distributionAlgorithms.indexOf(algo);
						if (i < (distributionAlgorithms.size() - 1))
						{
							algo =
								(DistributionAlgorithm)
									distributionAlgorithms.get(i + 1);
						}
						else
						{
							// No more algorithms available, disconnect
							// and continue on to the next client.
							logger.warning(
								"Unable to find a working target for client " +
								client);
							currentAlgorithm.remove(client);
							try { client.close(); } catch (IOException e) {}
							continue;
						}
					}

					// Record the current algorithm in case it fails to
					// find a working target and the client needs
					// another trip through this section
					currentAlgorithm.put(client, algo);

					//
					// Ask the algorithm to attempt to find a Target for
					// this client
					//
					logger.finer("Asking " + algo +
						" to try to find a target for " + client);
					algo.tryToConnect(client);
				}
			}

			//
			// Handle clients that distribution algorithms have
			// finished distributing
			//

			synchronized (finishedDistributing)
			{
				Iterator<Connection> iter = finishedDistributing.iterator();
				while (iter.hasNext())
				{
					Connection conn = iter.next();
					iter.remove();

					// Let each distribution algorithm know that a
					// successful connection has occurred.  Some
					// algorithms want to record that information.
					logger.finer(
						"Notifying distribution algorithms of successful " +
						"connection " + conn);
					Iterator<DistributionAlgorithm> algoIter = distributionAlgorithms.iterator();
					while (algoIter.hasNext())
					{
						DistributionAlgorithm algo = algoIter.next();
						algo.connectionNotify(conn);
					}

					// Yank them from currentAlgorithm
					currentAlgorithm.remove(conn.getClient());

					// Register them with the Target
					logger.finer(
						"Registering connection " + conn + "with target");
					conn.getTarget().addConnection(conn);
				}
			}
		}
	}

	protected String getMemoryStats(String indent)
	{
		String stats;

		stats = indent +
			currentAlgorithm.size() + " entries in currentAlgorithm Map\n";
		stats += indent +
			needsDistributing.size() + " entries in needsDistributing List\n";
		stats += indent +
			finishedDistributing.size() +
			" entries in finishedDistributing List";

		return stats;
	}
}