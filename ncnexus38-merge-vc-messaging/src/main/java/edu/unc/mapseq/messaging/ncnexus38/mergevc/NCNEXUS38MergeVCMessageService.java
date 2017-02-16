package edu.unc.mapseq.messaging.ncnexus38.mergevc;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCNEXUS38MergeVCMessageService {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38MergeVCMessageService.class);

    private Connection connection;

    private Session session;

    private ConnectionFactory connectionFactory;

    private NCNEXUS38MergeVCMessageListener messageListener;

    private String destinationName;

    public NCNEXUS38MergeVCMessageService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        this.connection = connectionFactory.createConnection();
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = this.session.createQueue(this.destinationName);
        MessageConsumer consumer = this.session.createConsumer(destination);
        consumer.setMessageListener(messageListener);
        this.connection.start();
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        if (this.session != null) {
            this.session.close();
        }
        if (this.connection != null) {
            this.connection.stop();
            this.connection.close();
        }
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public NCNEXUS38MergeVCMessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(NCNEXUS38MergeVCMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

}
