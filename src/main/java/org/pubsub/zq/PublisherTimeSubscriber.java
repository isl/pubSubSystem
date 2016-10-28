package org.pubsub.zq;

/**
 * Created by dmetallidis on 9/8/16.
 */

import org.kairosdb.client.builder.DataPoint;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.Date;
import java.util.List;

public class PublisherTimeSubscriber {

    public static final String rawStartAndEndEntityDelayMetricNames[] = {
            "userDelay_latency_milliseconds", "userAnswerDelay_latency_milliseconds",
            "permAdminDelay_latency_milliseconds", "permAdminAnswerDelay_latency_milliseconds",
            "objectDelay_latency_milliseconds", "objectAnswerDelay_latency_milliseconds",
            "ouUserDelay_latency_milliseconds", "ouUserAnswerDelay_latency_milliseconds",
            "roleDelay_latency_milliseconds", "roleAnswerDelay_latency_milliseconds",
            "userDetailDelay_latency_milliseconds", "userDetailAnswerDelay_latency_milliseconds",
            "permDelay_latency_milliseconds", "permAnswerDelay_latency_milliseconds",
            "objectAdminDelay_latency_milliseconds", "objectAdminAnswerDelay_latency_milliseconds",
            "roleAdminDelay_latency_milliseconds", "roleAdminAnswerDelay_latency_milliseconds",
            "ouPermDelay_latency_milliseconds", "ouPermAnswerDelay_latency_milliseconds",
            "sdDynamicDelay_latency_milliseconds", "sdDynamicAnswerDelay_latency_milliseconds",
            "sdStaticDelay_latency_milliseconds", "sdStaticAnswerDelay_latency_milliseconds"
    };

    public static final String rawSecurityMetrics[] = {
            "failedAuthentications",
            "totalAttemptsOfAuthentication"};

    public static final String prefixesReliabilityOfServices_mtbf [] = {"reliabilityUserPage_mtbf", "reliabilityPermAdminPage_mtbf", "reliabilityObjectPage_mtbf",
            "reliabilityOuUserPage_mtbf", "reliabilityRolePage_mtbf", "reliabilityUserDetailPage_mtbf", 
            "reliabilityPermPage_mtbf", "reliabilityObjectAdminPage_mtbf", "reliabilityRoleAdminPage_mtbf"
            , "reliabilityOuPermPage_mtbf", "reliabilitySdDynamicPage_mtbf", "reliabilitySdStaticPage_mtbf"};



    public static final String prefixesReliabilityOfServices_totalUpTime [] = {"reliabilityUserPagetotalUpTime",
            "reliabilityPermAdminPagetotalUpTime", "reliabilityObjectPagetotalUpTime",
            "reliabilityOuUserPagetotalUpTime", "reliabilityRolePagetotalUpTime", "reliabilityUserDetailPagetotalUpTime",
            "reliabilityPermPagetotalUpTime", "reliabilityObjectAdminPagetotalUpTime", "reliabilityRoleAdminPagetotalUpTime"
            , "reliabilityOuPermPagetotalUpTime", "reliabilitySdDynamicPagetotalUpTime", "reliabilitySdStaticPagetotalUpTime"};

    public static final String prefixesReliabilityOfServices_totalDownTime [] = {"reliabilityUserPagetotalDownTime",
            "reliabilityPermAdminPagetotalDownTime", "reliabilityObjectPagetotalDownTime",
            "reliabilityOuUserPagetotalDownTime", "reliabilityRolePagetotalDownTime", "reliabilityUserDetailPagetotalDownTime",
            "reliabilityPermPagetotalDownTime", "reliabilityObjectAdminPagetotalDownTime", "reliabilityRoleAdminPagetotalDownTime"
            , "reliabilityOuPermPagetotalDownTime", "reliabilitySdDynamicPagetotalDownTime", "reliabilitySdStaticPagetotalDownTime"};

    public static final String prefixesReliabilityOfServices_nBreakDowns [] = {"reliabilityUserPagenBreakDowns",
            "reliabilityPermAdminPagenBreakDowns", "reliabilityObjectPagenBreakDowns",
            "reliabilityOuUserPagenBreakDowns", "reliabilityRolePagenBreakDowns", "reliabilityUserDetailPagenBreakDowns",
            "reliabilityPermPagenBreakDowns", "reliabilityObjectAdminPagenBreakDowns", "reliabilityRoleAdminPagenBreakDowns"
            , "reliabilityOuPermPagenBreakDowns", "reliabilitySdDynamicPagenBreakDowns", "reliabilitySdStaticPagenBreakDowns"};


    //example1
    public static final String rawStartAndEndEntityDelayMetricNames1[] = { "fortress-web_availabilityPercentage"};

    //example2
    public static final String rawStartAndEndEntityDelayMetricNames2[] = {"exampleMetric"};

    public static void main(String[] args) throws Exception {

        // Prepare our context and publisher in order to publish the metric values
        // many-to-many sockets connections, many publishers which is the application servers to many subscribers which is for the
        // workflow machine sockets
        Context context = ZMQ.context(1);
        Socket publisher = context.socket(ZMQ.PUB);
        publisher.bind("tcp://*:5563");
        KairosDbClient kairosClient = new KairosDbClient("http://localhost:8088");


        // Prepare our context and subscriber in order to subscribe
        // this needs to change with the workflow machine IP
        // one-to-many connection , one from the publisher perspective of the workflow machine socket and many for each of the application servers ,
        // in our case is the tomcat 8080
        // the below socket is for the workflow of "workflowMonitoringExecution14" and waits for the start time of it
        Context sContext = ZMQ.context(1);
        Socket subscriber = sContext.socket(ZMQ.SUB);
//        subscriber.connect("tcp://192.168.254.132:5564");
          subscriber.connect("tcp://" +args[0]+ ":5564");
        subscriber.subscribe("workflowMonitoringExecution14".getBytes());


        // first we call the subscriber of workflow (one-to-many) :  "workflowMonitoringExecution14" in order to get startTime of the workflow
        String startTimeStamp = String.valueOf(new Date(0).getTime());
        while (!Thread.currentThread ().isInterrupted ()) {
            System.out.println("Waiting for the workflowname and the timestamp");
            String startWorkflowName = subscriber.recvStr();
            String timestampWorkflow = subscriber.recvStr();
            if(startWorkflowName.equals("workflowMonitoringExecution14") && timestampWorkflow != null){
                System.out.println("We have taken the workflowname and timestamp");
                startTimeStamp = timestampWorkflow;
                break;
            }

            Thread.sleep(3000);
        }


        // and now send every 3 seconds send the values to the subscriber from KairosDB
        // many to many connection for the workflow of workflowMonitoringExecution14
        while (!Thread.currentThread ().isInterrupted ()) {
            System.out.println("Sending the dependency values");
            sendStartEndEntityDelay(publisher, kairosClient, startTimeStamp);
            sendSecurityMetrics(publisher, kairosClient, startTimeStamp);
            sendMtbfMetrics(publisher, kairosClient, startTimeStamp);
            sendTotalUpTime(publisher, kairosClient, startTimeStamp);
            sendNumberOfBreakDowns(publisher, kairosClient, startTimeStamp);
            sendDownTimes(publisher, kairosClient, startTimeStamp);
//            Thread.sleep(3000);
        }

        publisher.close ();
        context.term ();

        subscriber.close ();
        sContext.term ();
    }

    private static void sendDownTimes(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {

        Long startWorkflowTime = Long.parseLong(startTimeStamp);
        Double dvalue;
        Object value;
        Double totalValue = 0.0;
        for (int i = 0; i < prefixesReliabilityOfServices_nBreakDowns.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(prefixesReliabilityOfServices_nBreakDowns[i], new Date(0), null);

            // we get the listDataPoints that have started after the execution of the workflow and them cause we consider a task calls
            // and not individual request calls
            if (listDapoint.size() > 0) {
                for (int j = 0; j < listDapoint.size(); j++) {
                    if(listDapoint.get(j).getTimestamp() >= startWorkflowTime){
                    value = listDapoint.get(j).getValue();
                    dvalue = Double.parseDouble(value.toString());
                    totalValue = totalValue + dvalue;
                    }
                }
            }
            publisher.sendMore(prefixesReliabilityOfServices_nBreakDowns[i]);
            publisher.send(totalValue.toString());
            totalValue  = 0.0;
            Thread.sleep(1000);
        }

    }

    private static  void sendNumberOfBreakDowns(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {


        Long startWorkflowTime = Long.parseLong(startTimeStamp);
        Double dvalue;
        Object value;
        Double totalValue = 0.0;

        for (int i = 0; i < prefixesReliabilityOfServices_totalDownTime.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(prefixesReliabilityOfServices_totalDownTime[i], new Date(0), null);

            // we get the listDataPoints that have started after the execution of the workflow and them cause we consider a task calls
            // and not individual request calls
            if (listDapoint.size() > 0) {
                for (int j = 0; j < listDapoint.size(); j++) {
                    if(listDapoint.get(j).getTimestamp() >= startWorkflowTime){
                        value = listDapoint.get(j).getValue();
                        dvalue = Double.parseDouble(value.toString());
                        totalValue = totalValue + dvalue;
                    }
                }
            }
            publisher.sendMore(prefixesReliabilityOfServices_totalDownTime[i]);
            publisher.send(totalValue.toString());
            totalValue  = 0.0;
            Thread.sleep(1000);
        }

    }

    private static void sendTotalUpTime(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {

        Long startWorkflowTime = Long.parseLong(startTimeStamp);
        Double dvalue;
        Object value;
        Double totalValue = 0.0;

        for (int i = 0; i < prefixesReliabilityOfServices_totalUpTime.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(prefixesReliabilityOfServices_totalUpTime[i], new Date(0), null);

            // we get the listDataPoints that have started after the execution of the workflow and them cause we consider a task calls
            // and not individual request calls
            if (listDapoint.size() > 0) {
                for (int j = 0; j < listDapoint.size(); j++) {
                    if(listDapoint.get(j).getTimestamp() >= startWorkflowTime){
                        value = listDapoint.get(j).getValue();
                        dvalue = Double.parseDouble(value.toString());
                        totalValue = totalValue + dvalue;
                    }
                }
            }
            publisher.sendMore(prefixesReliabilityOfServices_totalUpTime[i]);
            publisher.send(totalValue.toString());
            totalValue  = 0.0;
            Thread.sleep(1000);
        }

    }

    private static void sendMtbfMetrics(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {

        for (int i = 0; i < prefixesReliabilityOfServices_mtbf.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(prefixesReliabilityOfServices_mtbf[i], new Date(0), null);

            // here we are going to take the last value of the mtbf as we care for the most update value of the mtbf
            // that will occur for the workflow having just finished
            if (listDapoint.size() > 0) {
                    Object value = listDapoint.get(listDapoint.size()-1).getValue();
                    Double dvalue = Double.parseDouble(value.toString());
                    publisher.sendMore(prefixesReliabilityOfServices_mtbf[i]);
                    publisher.send(dvalue.toString());
                    Thread.sleep(1000);
                }

            }
        }


    private static void sendSecurityMetrics(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {


        Long startWorkflowTime = Long.parseLong(startTimeStamp);
        Double dvalue;
        Object value;
        Double totalValue = 0.0;

        for (int i = 0; i < rawSecurityMetrics.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(rawSecurityMetrics[i], new Date(0), null);

            // we get the listDataPoints that have started after the execution of the workflow and them cause we consider a task calls
            // and not individual request calls
            if (listDapoint.size() > 0) {
                for (int j = 0; j < listDapoint.size(); j++) {
                    if(listDapoint.get(j).getTimestamp() >= startWorkflowTime){
                        value = listDapoint.get(j).getValue();
                        dvalue = Double.parseDouble(value.toString());
                        totalValue = totalValue + dvalue;
                    }
                }
            }
            publisher.sendMore(rawSecurityMetrics[i]);
            publisher.send(totalValue.toString());
            totalValue  = 0.0;
            Thread.sleep(1000);
        }

    }


    private static void sendStartEndEntityDelay(Socket publisher, KairosDbClient kairosClient, String startTimeStamp) throws Exception {


        Long startWorkflowTime = Long.parseLong(startTimeStamp);
        Double dvalue;
        Object value;
        Double totalValue = 0.0;

        for (int i = 0; i < rawStartAndEndEntityDelayMetricNames.length; i++) {
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(rawStartAndEndEntityDelayMetricNames[i], new Date(0), null);

            // we get the listDataPoints that have started after the execution of the workflow and them cause we consider a task calls
            // and not individual request calls
            if (listDapoint.size() > 0) {
                for (int j = 0; j < listDapoint.size(); j++) {
                    if(listDapoint.get(j).getTimestamp() >= startWorkflowTime){
                        value = listDapoint.get(j).getValue();
                        dvalue = Double.parseDouble(value.toString());
                        totalValue = totalValue + dvalue;
                    }
                }
            }
            publisher.sendMore(rawStartAndEndEntityDelayMetricNames[i]);
            publisher.send(totalValue.toString());
            totalValue  = 0.0;
            Thread.sleep(1000);
        }
    }

}
