package com.netflix.turbine.discovery.zookeeper;

import com.netflix.turbine.Turbine;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhumingyuan on 22/8/17.
 */
public class StartZookeeperTurbine {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartZookeeperTurbine.class);

    private static final String ARG_TURBINE_PORT = "turbinePort";
    private static final String ARG_ZK_CONNECTION_STRING = "zkConnectionString";
    private static final String ARG_MEMBER_PATH = "memberPath";
    private static final String ARG_URL_TEMPLATE = "urlTemplate";

    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts(ARG_TURBINE_PORT).withOptionalArg();
        optionParser.accepts(ARG_ZK_CONNECTION_STRING).withOptionalArg();
        optionParser.accepts(ARG_URL_TEMPLATE).withOptionalArg();
        optionParser.accepts(ARG_MEMBER_PATH).withRequiredArg();

        OptionSet options = optionParser.parse(args);
        int port = -1;
        if (!options.has(ARG_TURBINE_PORT)) {
            System.err.println("Argument -turbinePort required for SSE HTTP server to start on. Eg. -turbinePort 8888");
            System.exit(-1);
        } else {
            try {
                port = Integer.parseInt(String.valueOf(options.valueOf(ARG_TURBINE_PORT)));
            } catch (NumberFormatException e) {
                System.err.println("Value of port must be an integer but was: " + options.valueOf(ARG_TURBINE_PORT));
            }
        }
        String zkConnectionString = "127.0.0.1:2181";
        if (options.has(ARG_ZK_CONNECTION_STRING)) {
            zkConnectionString = String.valueOf(options.valueOf(ARG_ZK_CONNECTION_STRING));
            LOGGER.info("zkConnection {}", zkConnectionString);
        }
        String memberPath = "";
        if (!options.has(ARG_MEMBER_PATH)) {
            System.err.println("Argument -memberPath required for Zookeeper instance discovery. Eg. -memberPath /production");
            System.exit(-1);
        } else {
            memberPath = String.valueOf(options.valueOf(ARG_MEMBER_PATH));
        }
        String template = "http://" + ZookeeperStreamDiscovery.HOSTNAME + "/turbine.stream";
        if (options.has("urlTemplate")) {
            template = String.valueOf(options.valueOf("urlTemplate"));
            if (!template.contains(ZookeeperStreamDiscovery.HOSTNAME)) {
                System.err.println("Argument -urlTemplate must contain " + ZookeeperStreamDiscovery.HOSTNAME + " marker. Eg. http://" + ZookeeperStreamDiscovery.HOSTNAME + "/hystrix.stream");
                System.exit(-1);
            }
        }

        LOGGER.info("Turbine => Zookeeper memberPath: " + memberPath);
        LOGGER.info("Turbine => Zookeeper urlTemplate: " + template);

        try {
            Turbine.startServerSentEventServer(port, ZookeeperStreamDiscovery.create(memberPath, zkConnectionString, template));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
