package com.datafibers.service;

import com.datafibers.util.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DFInitService {

    private static final Logger LOG = LoggerFactory.getLogger(DFInitService.class);

    public static void main(String[] args) {
        welcome();
        if (null == args || args.length == 0) { // Use all default start settings - standalone with web ui
            Runner.runExample(DFDataProcessor.class);
            LOG.info("Start DF Data Processor in standalone mode ...");
            Runner.runExample(DFWebUI.class);

        } else { // Use full parameters start
            if(args[0].equalsIgnoreCase("c") && args[1].equalsIgnoreCase("ui")) {
                Runner.runClusteredExample(DFDataProcessor.class);
                LOG.info("Start DF Data Processor in cluster mode...");
                Runner.runExample(DFWebUI.class);

            } else if(args[0].equalsIgnoreCase("c") && args[1].equalsIgnoreCase("no-ui")) {
                Runner.runClusteredExample(DFDataProcessor.class);
                LOG.info("Start DF Data Processor in cluster mode without Web UI...");

            } else if(args[0].equalsIgnoreCase("s") && args[1].equalsIgnoreCase("ui")) {
                Runner.runExample(DFDataProcessor.class);
                LOG.info("Start DF Data Processor in standalone mode ...");
                Runner.runExample(DFWebUI.class);

            } else if(args[0].equalsIgnoreCase("s") && args[1].equalsIgnoreCase("no-ui")) {
                Runner.runExample(DFDataProcessor.class);
                LOG.info("Start DF Data Processor in standalone mode without Web UI...");

            } else {
                System.err.println("Usage: java -jar DFDataProcessor.jar <SERVICE_TO_DEPLOY> <UI_Enabled>");
                System.err.println("Note:");
                System.err.println("<SERVICE_TO_DEPLOY>=s: Deploy DFDataProcessor vertical in standalone mode.");
                System.err.println("<SERVICE_TO_DEPLOY>=c: Deploy DFDataProcessor vertical in cluster mode.");
                System.err.println("<UI_Enabled>=ui: Deploy Web UI.");
                System.err.println("<UI_Enabled>=no-ui: Start without Web UI.");
                System.exit(0);
            }
        }
        LOG.info("Start DF Services Completed :)");

    }
    public static void welcome() {
        System.out.println(" __    __     _                            _             ___      _          ___ _ _                   \n" +
                "/ / /\\ \\ \\___| | ___ ___  _ __ ___   ___  | |_ ___      /   \\__ _| |_ __ _  / __(_) |__   ___ _ __ ___ \n" +
                "\\ \\/  \\/ / _ \\ |/ __/ _ \\| '_ ` _ \\ / _ \\ | __/ _ \\    / /\\ / _` | __/ _` |/ _\\ | | '_ \\ / _ \\ '__/ __|\n" +
                " \\  /\\  /  __/ | (_| (_) | | | | | |  __/ | || (_) |  / /_// (_| | || (_| / /   | | |_) |  __/ |  \\__ \\\n" +
                "  \\/  \\/ \\___|_|\\___\\___/|_| |_| |_|\\___|  \\__\\___/  /___,' \\__,_|\\__\\__,_\\/    |_|_.__/ \\___|_|  |___/\n" +
                "                                                                                                       ");
    }
}
