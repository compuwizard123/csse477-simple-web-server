/*
 * ConnectionComparator.java
 * Oct 28, 2012
 *
 * Simple Web Server (SWS) for EE407/507 and CS455/555
 * 
 * Copyright (C) 2011 Chandan Raj Rupakheti, Clarkson University
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
 * 
 * Contact Us:
 * Chandan Raj Rupakheti (rupakhcr@clarkson.edu)
 * Department of Electrical and Computer Engineering
 * Clarkson University
 * Potsdam
 * NY 13699-5722
 * http://clarkson.edu/~rupakhcr
 */
 
package server;

import java.util.Comparator;

import protocol.HttpRequest;

/**
 * @author atniptw
 * @author risdenkj
 * @author crawfonw
 */
public class RequestComparator implements Comparator<HttpRequest> {

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(HttpRequest o1, HttpRequest o2) {
		if(o1.getMethod().equalsIgnoreCase("GET") && !o2.getMethod().equalsIgnoreCase("GET")) {
			return 1;
		}
		else if(o2.getMethod().equalsIgnoreCase("GET") && !o1.getMethod().equalsIgnoreCase("GET")) {
			return -1;
		}
		else
			return 0;
	}



}
