package edu.unc.mapseq.messaging;

import java.io.IOException;
import java.io.StringWriter;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class MessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://152.54.3.109:61616");

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncnexus38.variantcalling");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "Sample");
            generator.writeStringField("id", "1774501");

            generator.writeArrayFieldStart("attributes");

            generator.writeStartObject();
            generator.writeStringField("name", "subjectName");
            generator.writeStringField("value", "NCG_00020");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "NCG_00020-ncnexus38-vc-test-3");
            generator.writeArrayFieldStart("attributes");

            generator.writeStartObject();
            generator.writeStringField("name", "sselProbe");
            generator.writeStringField("value", "6");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("name", "freeBayesJobCount");
            generator.writeStringField("value", "48");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();
            System.out.println(sw.toString());
            producer.send(session.createTextMessage(sw.toString()));

        } catch (JMSException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }
}
