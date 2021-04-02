package cpen431.mp.Tests;

import java.util.Random;

public class TestsConfig {

    // Default configurations
    private static Random RAND = new Random();
    private static int MAX_REPEAT = 10;
    private static int MAX_REPEAT_LITE = 10;
    private static String[] failedServerNodes;
    private static int CLIENTS_COUNT = 16;
    private static boolean strictIsAlive = true;
    private static int POP_NODES_THRESHOLD = 5;

    public static int getPopNodesThreshold() {
        return POP_NODES_THRESHOLD;
    }

    public static void setPopNodesThreshold(int popNodesThreshold) {
        POP_NODES_THRESHOLD = popNodesThreshold;
    }

    public static Random getRAND() {
        return RAND;
    }

    public static void setRAND(Random RAND) {
        TestsConfig.RAND = RAND;
    }

    public static int getMaxRepeat() {
        return MAX_REPEAT;
    }

    public static void setMaxRepeat(int maxRepeat) {
        MAX_REPEAT = maxRepeat;
    }

    public static int getMaxRepeatLite() {
        return MAX_REPEAT_LITE;
    }

    public static void setMaxRepeatLite(int maxRepeatLite) {
        MAX_REPEAT_LITE = maxRepeatLite;
    }

    public static String[] getFailedServerNodes() {
        return failedServerNodes;
    }

    public static void setFailedServerNodes(String[] failedServerNodes) {
        TestsConfig.failedServerNodes = failedServerNodes;
    }

    public static int getClientsCount() {
        return CLIENTS_COUNT;
    }

    public static void setClientsCount(int clientsCount) {
        CLIENTS_COUNT = clientsCount;
    }

    public static boolean isStrictIsAlive() {
        return strictIsAlive;
    }

    public static void setStrictIsAlive(boolean strictIsAlive) {
        TestsConfig.strictIsAlive = strictIsAlive;
    }

}

