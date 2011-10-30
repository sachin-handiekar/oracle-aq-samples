package com.sachinhandiekar.oracle.aq;

import java.sql.Connection;
import java.sql.SQLException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jms.AQjmsAdtMessage;
import oracle.jms.AQjmsDestination;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsQueueSender;
import oracle.jms.AQjmsSession;
import oracle.sql.ORADataFactory;
import oracle.xdb.XMLType;

public class JMSAQTest {

	private static final String queueOwner = "owner";

	private static final String queueName = "queue";

	public static void main(String[] args) {
		try {
			enqueueMessage("<sample>hello sachin</sample>");
			dequeueMessage();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void enqueueMessage(String xmlMessage) throws JMSException, SQLException {
		QueueConnectionFactory q_cf = null;
		QueueConnection q_conn = null;
		QueueSession q_sess = null;
		QueueSender sender = null;
		Queue queue = null;

		q_cf = AQjmsFactory.getQueueConnectionFactory(getOracleDataSource());
		q_conn = q_cf.createQueueConnection();
		q_sess = q_conn.createQueueSession(true, Session.CLIENT_ACKNOWLEDGE);
		q_conn.start();

		queue = ((AQjmsSession) q_sess).getQueue(queueOwner, queueName);
		((AQjmsDestination) queue).start(q_sess, true, true);

		sender = (AQjmsQueueSender) q_sess.createSender(queue);

		Connection conn = getOracleDataSource().getConnection();
		XMLType payload = XMLType.createXML(conn, xmlMessage);
		conn.close();

		Message msg = ((AQjmsSession) q_sess).createORAMessage(payload);

		sender.send(msg);
		System.out.println("Message Enqueued ==> " + payload.getStringVal());

		q_sess.commit();
		q_sess.close();
		q_conn.close();

	}

	public static void dequeueMessage() throws JMSException, SQLException {
		QueueConnectionFactory q_cf = null;
		QueueConnection q_conn = null;
		QueueReceiver receiver = null;
		QueueSession q_sess = null;
		Message message = null;
		Queue queue = null;

		q_cf = AQjmsFactory.getQueueConnectionFactory(getOracleDataSource());
		q_conn = q_cf.createQueueConnection();
		q_sess = q_conn.createQueueSession(true, Session.CLIENT_ACKNOWLEDGE);

		q_conn.start();

		queue = ((AQjmsSession) q_sess).getQueue(queueOwner, queueName);
		((AQjmsDestination) queue).start(q_sess, true, true);

		ORADataFactory orad = XMLType.getORADataFactory();
		receiver = ((AQjmsSession) q_sess).createReceiver(queue, orad);
		message = receiver.receiveNoWait();

		if (message == null) {
			System.out.println("no messages");
		}
		else {

			XMLType xmlMessage = (XMLType) ((AQjmsAdtMessage) message).getAdtPayload();
			System.out.println("Message Dequeued ==> " + xmlMessage.getStringVal());
			q_sess.commit();

		}

		q_sess.close();
		q_conn.close();
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
