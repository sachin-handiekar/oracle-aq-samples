package com.sachinhandiekar.oracle.aq;

import java.sql.SQLException;

import org.bouncycastle.util.encoders.Hex;

import oracle.AQ.AQDequeueOption;
import oracle.AQ.AQDriverManager;
import oracle.AQ.AQEnqueueOption;
import oracle.AQ.AQException;
import oracle.AQ.AQMessage;
import oracle.AQ.AQObjectPayload;
import oracle.AQ.AQOracleQueue;
import oracle.AQ.AQQueue;
import oracle.AQ.AQSession;
import oracle.jdbc.pool.OracleDataSource;
import oracle.sql.ORADataFactory;
import oracle.xdb.XMLType;

public class AQTest {

	private static final String queueOwner = "owner";

	private static final String queueName = "queue";

	public static void main(String[] args) {

		AQTest aqTest = new AQTest();

		String xmlMessage = "<sample>hello sachin</sample>";
		try {
			aqTest.enqueueMessage(xmlMessage);
			aqTest.dequeueMessage();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void enqueueMessage(String xmlMessage) throws SQLException, AQException, ClassNotFoundException {
		java.sql.Connection aqconn = getOracleDataSource().getConnection();
		aqconn.setAutoCommit(false);

		AQSession aqsession = null;

		// Register the Oracle AQ Driver
		Class.forName("oracle.AQ.AQOracleDriver");
		try {
			AQEnqueueOption enqueueOption = new AQEnqueueOption();

			aqsession = AQDriverManager.createAQSession(aqconn);
			AQQueue queue = aqsession.getQueue(queueOwner, queueName);
			AQMessage msg = ((AQOracleQueue) queue).createMessage();

			AQObjectPayload payload = msg.getObjectPayload();
			XMLType payloadData = XMLType.createXML(aqconn, xmlMessage);
			payload.setPayloadData(payloadData);

			queue.enqueue(enqueueOption, msg);
			aqconn.commit();
			System.out.println("Message succesfully enqueued..");
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		finally {
			aqsession.close();
			aqconn.close();
		}
	}

	public void dequeueMessage() throws AQException, SQLException, ClassNotFoundException {
		java.sql.Connection aqconn = getOracleDataSource().getConnection();
		aqconn.setAutoCommit(false);

		AQSession aq_sess = null;

		Class.forName("oracle.AQ.AQOracleDriver");

		try {
			aq_sess = AQDriverManager.createAQSession(aqconn);

			AQQueue queue;
			AQMessage message;
			AQDequeueOption deq_option;

			queue = aq_sess.getQueue(queueOwner, queueName);

			deq_option = new AQDequeueOption();

			// receive a message via native interface
			ORADataFactory factory = XMLType.getORADataFactory();

			message = ((AQOracleQueue) queue).dequeue(deq_option, factory);

			if (message == null) {
				System.out.println("no messages");
			}
			else {
				System.out.println("Successful dequeue");

				System.out.println("message id: " + new String(Hex.encode(message.getMessageId())));

				XMLType messageData = (XMLType) message.getObjectPayload().getPayloadData();
				String xmlString = messageData.getStringVal();
				System.out.println("content: " + xmlString);
				
				//Commit
				aqconn.commit();

			}
		}
		finally {
			aq_sess.close();
			aqconn.close();
		}

	}

	public static OracleDataSource getOracleDataSource() throws SQLException {
		OracleDataSource ds = new OracleDataSource();
		ds.setDriverType("thin");
		ds.setServerName("localhost");
		ds.setPortNumber(1521);
		ds.setDatabaseName("xe"); // sid
		ds.setUser("username");
		ds.setPassword("password");

		return ds;
	}

}
