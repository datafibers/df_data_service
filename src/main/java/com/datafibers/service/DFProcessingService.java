package com.datafibers.service;

import com.datafibers.util.Runner;

/**
 * Core service entry to deploy vertical
 */
public class DFProcessingService {

    public static void main(String[] args) {

        if (null == args) {
            System.err.println("Usage: java -jar DFProcessingService.jar <SERVICE_TO_DEPLOY>");
            System.err.println("Note:");
            System.err.println("<SERVICE_TO_DEPLOY>=\"s-all\": Deploy both Producer & Transform vertical in this host as single mode.");
            System.err.println("<SERVICE_TO_DEPLOY>=\"s-c\": Deploy only Producer vertical in this host as single mode.");
            System.err.println("<SERVICE_TO_DEPLOY>=\"s-t\": Deploy only Transform vertical in this host as single mode.");
            System.err.println("<SERVICE_TO_DEPLOY>=\"c-all\": Deploy both Producer & Transform vertical in this host as cluster mode.");
            System.err.println("<SERVICE_TO_DEPLOY>=\"c-c\": Deploy only Producer vertical in this host as cluster mode.");
            System.err.println("<SERVICE_TO_DEPLOY>=\"c-t\": Deploy only Transform vertical in this host as cluster mode.");
            System.exit(0);
        } else {
            switch (args[0].toLowerCase()) {
                case "s-all":
                    Runner.runExample(DFProducer.class);
                    Runner.runExample(DFTransformer.class);
                    break;
                case "s-c":
                    Runner.runExample(DFProducer.class);
                    break;
                case "s-t":
                    Runner.runExample(DFTransformer.class);
                    break;
                case "c-all":
                    Runner.runClusteredExample(DFProducer.class);
                    Runner.runClusteredExample(DFTransformer.class);
                    break;
                case "c-c":
                    Runner.runClusteredExample(DFProducer.class);
                    break;
                case "c-t":
                    Runner.runClusteredExample(DFTransformer.class);
                    break;
            }
        }
    }

}
