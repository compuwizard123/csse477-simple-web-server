/*
 * DOSAttackWindow.java
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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SpringLayout;
import javax.swing.WindowConstants;

/**
 * This is the application window for {@link DOSClient}.
 *
 * @author  Chandan R. Rupakheti (rupakhet@rose-hulman.edu)
 */
public class DOSAttackWindow extends JFrame {
	private static final long serialVersionUID = -259879532218338842L;
	
	private class ServiceRateUpdater implements Runnable {
		public boolean stop = false;
		public void run() {
			while(!stop) {
				// Poll if client is not null
				if(client != null) {
					double rate = client.getServiceRate();
					if(rate == Double.MIN_VALUE)
						DOSAttackWindow.this.txtServiceRate.setText("Unknown");
					else
						DOSAttackWindow.this.txtServiceRate.setText(Double.toString(rate));
				}
				
				// Poll at an interval of 500 milliseconds
				try {
					Thread.sleep(500);
				}
				catch(Exception e){}
			}
		}
	}
	
	private DOSClient client;
	private ServiceRateUpdater rateUpdater;
	
	private JPanel panelServerConfig;
	private JLabel lblServerName;
	private JTextField txtServerName;
	private JLabel lblPortNumber;
	private JTextField txtPortNumber;
	
	private JPanel panelUpdate;
	private JLabel lblURI;
	private JTextField txtURI;
	private JLabel lblDelay;
	private JTextField txtDelay;

	private JPanel panelCommand;
	private JButton butStartServer;
	private JButton butStopServer;
	private JLabel lblServiceRate;
	private JTextField txtServiceRate;
	
	
	public DOSAttackWindow() {
		this.initComponents();
		this.addListeners();
	}
	
	private void initComponents() {
		this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		this.setTitle("Simple Denial-Of-Service Attack Client (SDSC)");

		// Server configuration panel
		this.panelServerConfig = new JPanel();
		this.panelServerConfig.setBorder(BorderFactory.createTitledBorder("Remote Server Configurations"));
		this.lblServerName = new JLabel("Server Name");
		this.txtServerName = new JTextField("");
		this.txtServerName.setPreferredSize(new Dimension(300, 21));
		this.lblPortNumber = new JLabel("Port Number");
		this.txtPortNumber = new JTextField("8080");
		
		// Layout server configuration panel
		this.panelServerConfig.setLayout(new SpringLayout());
		this.panelServerConfig.add(this.lblServerName);		
		this.panelServerConfig.add(this.txtServerName);		
		this.panelServerConfig.add(this.lblPortNumber);		
		this.panelServerConfig.add(this.txtPortNumber);

		// Compact server configuration panel
		SpringUtilities.makeCompactGrid(this.panelServerConfig, 2, 2, 5, 5, 5, 5);
		
		// Update parameter panel
		this.panelUpdate = new JPanel();
		this.panelUpdate.setBorder(BorderFactory.createTitledBorder("Updatable Parameters (type and press return)"));
		this.lblURI = new JLabel("URI to GET");
		this.txtURI = new JTextField("/");
		this.txtURI.setPreferredSize(new Dimension(300, 21));
		this.lblDelay = new JLabel("Connection Interval (ms)");
		this.txtDelay = new JTextField("500");

		// Layout parameter panel
		this.panelUpdate.setLayout(new SpringLayout());
		this.panelUpdate.add(this.lblURI);
		this.panelUpdate.add(this.txtURI);
		this.panelUpdate.add(this.lblDelay);
		this.panelUpdate.add(this.txtDelay);

		// Compact parameter panel
		SpringUtilities.makeCompactGrid(this.panelUpdate, 2, 2, 5, 5, 5, 5);
		
		// Command center panel
		this.panelCommand = new JPanel();
		this.panelCommand.setBorder(BorderFactory.createTitledBorder("Command Center"));
		this.butStartServer = new JButton("Start DOS Attack");
		this.butStopServer = new JButton("Stop DOS Attack");
		this.butStopServer.setEnabled(false);
		this.lblServiceRate = new JLabel("Service Rate (Connections Serviced/Second)");
		this.txtServiceRate = new JTextField("Unknown");

		// Layout command center panel
		this.panelCommand.setLayout(new SpringLayout());
		this.panelCommand.add(this.butStartServer);
		this.panelCommand.add(this.butStopServer);
		this.panelCommand.add(this.lblServiceRate);
		this.panelCommand.add(this.txtServiceRate);

		// Compact the grid
		SpringUtilities.makeCompactGrid(this.panelCommand, 2, 2, 5, 5, 5, 5);
		
		JPanel contentPane = (JPanel)this.getContentPane();
		contentPane.add(this.panelServerConfig, BorderLayout.NORTH);
		contentPane.add(this.panelUpdate, BorderLayout.CENTER);
		contentPane.add(this.panelCommand, BorderLayout.SOUTH);
		
		this.pack();
	}
	
	private void addListeners() {
		// Start DOS attack
		this.butStartServer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(client != null) {
					JOptionPane.showMessageDialog(DOSAttackWindow.this, "Attack is still in progress. First stop!", "Attack in Progress", JOptionPane.ERROR_MESSAGE);
					return;
				}
				
				int port = 8080;
				try {
					port = Integer.parseInt(DOSAttackWindow.this.txtPortNumber.getText());
				}
				catch(Exception ex) {
					JOptionPane.showMessageDialog(DOSAttackWindow.this, "Invalid Port Number!", "Server Configuration Error", JOptionPane.ERROR_MESSAGE);
					return;
				}

				long delay = 500;
				try {
					delay = Long.parseLong(DOSAttackWindow.this.txtDelay.getText());
				}
				catch(Exception ex) {
					JOptionPane.showMessageDialog(DOSAttackWindow.this, "Invalid Delay Value!", "Dealy Value Problem", JOptionPane.ERROR_MESSAGE);
					return;
				}
				
				String server = DOSAttackWindow.this.txtServerName.getText();
				client = new DOSClient(DOSAttackWindow.this, server, port);
				client.setUri(DOSAttackWindow.this.txtURI.getText());
				client.setDelay(delay);
				
				disableWidgets();
				
				// Run the client in a thread
				new Thread(client).start();
				
				rateUpdater = new ServiceRateUpdater();
				// Run the service rate update thread
				new Thread(rateUpdater).start();
			}
		});
		
		// Stop DOS Attack
		this.butStopServer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(client == null)
					return;
				client.stop();
				rateUpdater.stop = true;
				
				enableWidgets();
				
				client = null;
				rateUpdater = null;
			}
		});
		
		// Listen to the text box change
		this.txtURI.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String uri = txtURI.getText();
				if(!uri.startsWith("/")) {
					uri = "/" + uri;
				}
				if(client != null) {
					client.setUri(uri);
				}
			}
		});

		// Listen to the delay change
		this.txtDelay.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					long delay = Long.parseLong(txtDelay.getText());
					if(client != null) {
						client.setDelay(delay);
					}
				}
				catch(Exception ex) {
					JOptionPane.showMessageDialog(DOSAttackWindow.this, "Invalid Delay Value!", "Dealy Value Problem", JOptionPane.ERROR_MESSAGE);
				}
			}
		});
		
		// Add window closing listener, to close things properly
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				if(client != null) {
					client.stop();
					client = null;
				}
				if(rateUpdater != null) {
					rateUpdater.stop = true;
					rateUpdater = null;
				}
			}
		});
	}
	
	/**
	 * For displaying exception.
	 * @param e
	 */
	public void showSocketException(Exception e) {
		JOptionPane.showMessageDialog(DOSAttackWindow.this, e.getMessage(), "Socket Exception Occured", JOptionPane.ERROR_MESSAGE);
		this.client.stop();
		this.client = null;
		this.enableWidgets();
	}
	
	private void disableWidgets() {
		this.panelServerConfig.setEnabled(false);
		this.butStartServer.setEnabled(false);
		this.butStopServer.setEnabled(true);
	}

	private void enableWidgets() {
		this.panelServerConfig.setEnabled(true);
		this.butStartServer.setEnabled(true);
		this.butStopServer.setEnabled(false);
	}
	
	/**
	 * Application entry point that will open up GUI.
	 * @param args
	 */
	public static void main(String[] args) {
		java.awt.EventQueue.invokeLater(new Runnable() {
			public void run() {
				new DOSAttackWindow().setVisible(true);
			}
		});
	}
}
