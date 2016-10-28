package org.pubsub.zq;

import org.zeromq.ZMQ;

import java.util.Date;

/**
 * Created by dmetallidis on 9/12/16.
 */
public class startPointPublisher {

    public static void main(String[] args) throws Exception {

        // Prepare our context and publisher in order to publish the metric values
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket publisher = context.socket(ZMQ.PUB);
        publisher.bind("tcp://*:5564");
        KairosDbClient kairosClient = new KairosDbClient("http://localhost:8088");

        // and now send every XX seconds send the values to the subscriber from KairosDB
        while (!Thread.currentThread ().isInterrupted ()) {

            publisher.sendMore("startWorkflowInstance");
            publisher.send(String.valueOf(new Date().getTime()));
            Thread.sleep(3000);
        }
        publisher.close ();
        context.term ();

    }
}
