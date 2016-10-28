package org.pubsub.zq;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Created by dmetallidis on 9/8/16.
 */
public class Subscriber {


    public static final String rawStartAndEndEntityDelayMetricNames1[] = { "fortress-web_availabilityPercentage"};
    //example2
    public static final String rawStartAndEndEntityDelayMetricNames2[] = {"workflowMonitoringExecution14"};

    public static void main (String[] args) throws InterruptedException {

        // Prepare our context and subscriber
        Context context = ZMQ.context(1);
        Socket subscriber = context.socket(ZMQ.SUB);

        subscriber.connect("tcp://192.168.182.129:5564");
//        subscriber.subscribe("A".getBytes());
//        subscriber.subscribe(rawStartAndEndEntityDelayMetricNames1[0].getBytes());
        subscriber.subscribe(rawStartAndEndEntityDelayMetricNames2[0].getBytes());

        while (!Thread.currentThread ().isInterrupted ()) {
            // Read envelope with address
            String address = subscriber.recvStr ();
            // Read message contents
            String contents = subscriber.recvStr ();
            System.out.println(address + " : " + contents);
        }
        subscriber.close ();
        context.term ();
    }

}
