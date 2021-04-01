package cpen431.mp.Tests;

import cpen431.mp.Statistics.KVSTestStats;
import cpen431.mp.Utilities.OSUtils;
import cpen431.mp.Utilities.ServerNode;
import cpen431.mp.KeyValue.KVSResultField;
import cpen431.mp.Utilities.PoPUtils;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import static cpen431.mp.Utilities.PoPUtils.checkSumForAllFiles;
import static cpen431.mp.Utilities.PoPUtils.checkSumForSingleJar;

public class TestDriver {

    // static methods

    /**
     * @param bigListFileName
     * @throws IOException
     */

    private static final String resultServiceURL = "http://34.94.48:43107";
//    private static final String gcInstanceMetadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/";
//
//    public static String getGCInstanceType() {
//        String instanceType = "NA";
//
//        try {
//            URL url = new URL(gcInstanceMetadataURL + "instance-type");
//            HttpURLConnection con = (HttpURLConnection) url.openConnection();
//            con.setRequestProperty("CustomHeader", "Metadata-Flavor: Google");
//            int responseCode = con.getResponseCode();
//
//            System.out.println(responseCode);
//
//            if (responseCode != HttpURLConnection.HTTP_OK) {
//                return instanceType;
//            }
//
//            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//            String inputLine;
//            StringBuffer response = new StringBuffer();
//
//            while ((inputLine = in.readLine()) != null) {
//                response.append(inputLine);
//            }
//            in.close();
//            con.disconnect();
//
//            instanceType = response.toString();
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            instanceType = "NA";
//        }
//
//        System.err.println(instanceType.trim());
//
//        return instanceType.trim();
//    }

    public static Boolean postResultsJSON(String secret, ArrayList<KVSResultField> resultMap) {
        try {
            JSONObject request_json = new JSONObject();
            request_json.put("Secret", secret);

            for (KVSResultField result : resultMap) {
                request_json.put(result.key, result.value);
            }

            byte[] request_bytes = request_json.toString().getBytes();

            URL url = new URL(resultServiceURL + "/submit");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            os.write(request_bytes);
            os.flush();

            int response_code = conn.getResponseCode();
            if (response_code != HttpURLConnection.HTTP_OK) {
                return false;
            }

            conn.disconnect();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static Boolean postSecretJSON(String secret) {
        try {
            JSONObject request_json = new JSONObject();
            request_json.put("Secret", secret);
            byte[] request_bytes = request_json.toString().getBytes();

            URL url = new URL(resultServiceURL + "/login");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            os.write(request_bytes);
            os.flush();

            int response_code = conn.getResponseCode();
            if (response_code != HttpURLConnection.HTTP_OK) {
                return false;
            }

            conn.disconnect();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String getClassBuildTime() {
        Date d = null;
        Class<?> currentClass = new Object() {
        }.getClass().getEnclosingClass();
        URL resource = currentClass.getResource(currentClass.getSimpleName() + ".class");
        if (resource != null) {
            if (resource.getProtocol().equals("file")) {
                try {
                    d = new Date(new File(resource.toURI()).lastModified());
                } catch (URISyntaxException ignored) {
                }
            } else if (resource.getProtocol().equals("jar")) {
                String path = resource.getPath();
                d = new Date(new File(path.substring(5, path.indexOf("!"))).lastModified());
            } else if (resource.getProtocol().equals("zip")) {
                String path = resource.getPath();
                File jarFileOnDisk = new File(path.substring(0, path.indexOf("!")));
                try (JarFile jf = new JarFile(jarFileOnDisk)) {
                    ZipEntry ze = jf.getEntry(path.substring(path.indexOf("!") + 2));//Skip the ! and the /
                    long zeTimeLong = ze.getTime();
                    Date zeTimeDate = new Date(zeTimeLong);
                    d = zeTimeDate;
                } catch (IOException | RuntimeException ignored) {
                }
            }
        }

        return (d != null ? d.toString() : "Unknown");
    }

    private static String getFileName(ArrayList<ServerNode> serverNodes) {
        String fileName = "";

        for (ServerNode s : serverNodes) {
            fileName += s.getHostName() + "_" + s.getPortNumber() + "_";
        }

        return fileName;
    }

    public static void executeM3SmokeTests(String bigListFileName, String secret, boolean exclude1024, boolean exclude2048) throws IOException {
        final long PERF_TEST_TIMER = 60 * 1000;
        final long CAPA_TEST_TIMER = 600 * 1000;
        final int[] BASIC_TEST_VALUE_SIZES = new int[]{8, 32, 128, 256};
        final int[] CLIENT_TYPES_LOCAL_SERVER = new int[]{1, 64, 512};
        final int[] CLIENT_TYPES_SINGLE_SERVER = new int[]{1, 16, 32};
        final int[] CLIENT_TYPES = new int[]{1, 16, 32, 64, 128, 256, 512, 1024, 2048};
        final int SUBMIT_NODE_COUNT = 35;
        final int SHUTDOWN_NODE_COUNT = 20;
        final double LOWERBOUND_ALLOWED = 0.0;
        final double UPPERBOUND_ALLOWED = 0.8;
        final int CAPACITY_INPUT = 20 * 1000;
        final int CAPACITY_INPUT_SIZE = 1000;
        final int PAUSE_SECONDS = 20;
        final int MAX_KEYS_SC = 100;
        final String GCP_SUBMIT_INSTANCE = "e2-standard-16";

        System.out.println("[Milestone 3 Smoke Tests]");
        System.out.println("Exclude 1024 clients tests: " + exclude1024);
        System.out.println("Exclude 2048 clients tests: " + exclude2048);

        ArrayList<ServerNode> bigListServerNodes = TestUtils.getServerNodes(bigListFileName);
        if (bigListServerNodes == null)
            return;
        System.out.println("The deployment has " + bigListServerNodes.size() + " server nodes.");

        if (bigListServerNodes.size() < SUBMIT_NODE_COUNT) {
            System.err.println("Error! Need to run on at least " + SUBMIT_NODE_COUNT + " nodes to execute the tests.");
            System.err.println("If you want to test on a smaller deployment, please use one of the previous evaluation clients.");
            return;
        }

        System.out.println("Checking that the used nodes are not duplicated");
        if (!ServerNode.ServersAreUnique(bigListServerNodes)) {
            System.err.println("Warning: You are not allowed to have duplicate nodes in the servers.txt file!");
            if (!secret.equals("0")) {
                System.err.println("Error: Tests aborted since running in submit mode!");
                return;
            }
        }

        // Create a list with the relevant results (to be submitted to the results collection server)
        ArrayList<KVSResultField> resultMap = new ArrayList<>();
        resultMap.add(new KVSResultField("Test Start Time", new Date().toString()));
        // resultMap.add(new KVSResultField("Client GC Instance Type", instanceType));
        resultMap.add(new KVSResultField("Client Local IP", InetAddress.getLocalHost().toString()));
        resultMap.add(new KVSResultField("Client Local Hostname", InetAddress.getLocalHost().getHostName()));
        resultMap.add(new KVSResultField("Server List", getFileName(bigListServerNodes)));
        resultMap.add(new KVSResultField("Server Count", Integer.toString(bigListServerNodes.size())));

        System.out.println("Running in test mode ... PoP will not check the process ID");
        if (TestsPool.testGetPIDAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
            System.err.println("Servers Process ID retrieval was unsuccessful. Aborting.");
            return;
        }

        // Compute the checksum for the students code
        HashMap<String, String> md5Results = new HashMap<>();
        md5Results.put(checkSumForAllFiles, "Null");      // Initialize the result map
        md5Results.put(checkSumForSingleJar, "Null");     // Initialize the result map

        // Ensure first server is alive.
        if (TestsPool.testIfSingleServerAlive(bigListServerNodes.get(0)) != TestStatus.TEST_PASSED) {
            System.out.println(">>> Server node " + bigListServerNodes.get(0).getHostName() + " is not behaving as expected.");
            System.err.println("First server is down. Aborting.");
            return;
        }

        // Wipe-out first -- ensure kvs is clear.
        if (TestsPool.testWipeoutAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
            System.err.println("Unsuccessful wipe-out. Aborting.");
            return;
        }

        // Ensure basic functionality.
        Tests.testUtilStartTimer();
        for (int valueLength : BASIC_TEST_VALUE_SIZES) {
            TestStatus testBasic = TestsPool.testSingleBasic(bigListServerNodes.get(0), valueLength);
            System.out.println(testBasic);

            if (testBasic != TestStatus.TEST_PASSED) {
                System.out.println(">>> Server node " + bigListServerNodes.get(0).getHostName() + " is not behaving as expected.");
                System.err.println("Potentially buggy implementation. Aborting!");
                return;
            }
        }
        resultMap.add(new KVSResultField("Test Single Front-End Basic", "PASSED"));
        Tests.testUtilElapsedTime();

        Tests.testUtilStartTimer();
        for (int valueLength : BASIC_TEST_VALUE_SIZES) {
            TestStatus testBasic = TestsPool.testDistributedBasic(bigListServerNodes, valueLength);
            System.out.println(testBasic);

            if (testBasic != TestStatus.TEST_PASSED) {
                System.err.println("Potentially buggy implementation. Aborting!");
                return;
            }
        }
        resultMap.add(new KVSResultField("Test Random Front-End Basic", "PASSED"));
        Tests.testUtilElapsedTime();

        // Test at-most-once policy.
        Tests.testUtilStartTimer();
        TestStatus statusAtMostOnce = TestsPool.testAtMostOncePolicy(bigListServerNodes);
        switch (statusAtMostOnce) {
            case TEST_PASSED:
                resultMap.add(new KVSResultField("Test Single Server At-Most-Once Client Policy", "PASSED"));
                break;
            case TEST_UNDECIDED:
                System.out.println(">>> Server node " + bigListServerNodes.get(0).getHostName() + " is not behaving as expected.");
                resultMap.add(new KVSResultField("Test Single Server At-Most-Once Client Policy", "UNDECIDED"));
                break;
            default:
                System.out.println(">>> Server node " + bigListServerNodes.get(0).getHostName() + " is not behaving as expected.");
                resultMap.add(new KVSResultField("Test Single Server At-Most-Once Client Policy", "FAILED"));
        }
        System.out.println(statusAtMostOnce);
        Tests.testUtilElapsedTime();


        // Ensure basic functionality.
        Tests.testUtilStartTimer();
        for (int valueLength : BASIC_TEST_VALUE_SIZES) {
            TestStatus testBasic = TestsPool.testSingleBasicVersion(bigListServerNodes.get(0), valueLength);
            System.out.println(testBasic);

            if (testBasic != TestStatus.TEST_PASSED) {
                System.out.println(">>> Server node " + bigListServerNodes.get(0).getHostName() + " is not behaving as expected.");
                System.err.println("Potentially buggy implementation. Aborting!");
                return;
            }
        }
        resultMap.add(new KVSResultField("Test Single Front-End Basic Version", "PASSED"));
        Tests.testUtilElapsedTime();

        Tests.testUtilStartTimer();
        for (int valueLength : BASIC_TEST_VALUE_SIZES) {
            TestStatus testBasic = TestsPool.testDistributedBasicVersion(bigListServerNodes, valueLength);
            System.out.println(testBasic);

            if (testBasic != TestStatus.TEST_PASSED) {
                System.err.println("Potentially buggy implementation. Aborting!");
                return;
            }
        }
        resultMap.add(new KVSResultField("Test Random Front-End Basic Version", "PASSED"));
        Tests.testUtilElapsedTime();

        if (TestsPool.testWipeoutAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
            System.err.println("Unsuccessful wipe-out. Aborting.");
            return;
        }

        System.out.println("Waiting for a few seconds.");
        Tests.pause(6);

        // Single server performance test.
        for (int clients : CLIENT_TYPES_SINGLE_SERVER) {
            Tests.testUtilStartTimer();
            KVSTestStats testSingleServerStats = new KVSTestStats();
            TestsPool.testSinglePerformanceTimed(bigListServerNodes.get(0), clients, testSingleServerStats, PERF_TEST_TIMER);
            if (testSingleServerStats.getResponseCount() != 0) {
                testSingleServerStats.printAggregateSummaryStats(resultMap, "Stage 1 Single Front-End");
            } else {
                System.err.println("No response statistics collected!");
            }
            // Wipe-out.
            if (TestsPool.testWipeoutAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipe-out. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Distributed performance test.
        for (int clients : CLIENT_TYPES) {
            Tests.testUtilStartTimer();
            TestStatus status;
            KVSTestStats testStats = new KVSTestStats();
            if ((clients < 1024) || (clients == 1024 && !exclude1024) || (clients == 2048 && !exclude2048)) {
                status = TestsPool.testDistributedPerformanceTimed(bigListServerNodes, clients, testStats, PERF_TEST_TIMER);
            } else {
                status = TestStatus.TEST_PASSED;
            }

            System.out.println(status);

            String s1 = "NA";
            if (clients >= 32) {
                s1 = OSUtils.loadavg();
            }

            if (testStats.getResponseCount() != 0) {
                testStats.printAggregateSummaryStats(resultMap, "Stage 1 Random Front-End", s1, clients);
            } else {
                System.err.println("No response statistics collected for " + clients + " clients!");
            }

            if (status != TestStatus.TEST_PASSED) {
                System.err.println("Test did not pass. Aborting. Maximum Clients = " + clients);
                return;
            }

            // Wipe-out.
            if (TestsPool.testWipeoutAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipe-out. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Insert capacity.
        Tests.testUtilStartTimer();
        TestStatus capStatus = TestsPool.testInsertCapacity(bigListServerNodes, CAPACITY_INPUT_SIZE,
                CAPACITY_INPUT, CAPA_TEST_TIMER, resultMap, TestsConfig.getClientsCount());
        if (capStatus != TestStatus.TEST_PASSED) {
            System.err.println("Capacity insert failed. Aborting.");
            return;
        }
        System.out.println(capStatus);
        Tests.testUtilElapsedTime();

        TestsConfig.setStrictIsAlive(false); // 'is alive' tests are relaxed from this point onwards

        // First Crash test -- first, set up list of nodes to crash.
        ArrayList<ServerNode> crashList = new ArrayList<ServerNode>();
        ArrayList<ServerNode> aliveList = new ArrayList<ServerNode>();
        ArrayList<ServerNode> tempList = new ArrayList<ServerNode>(bigListServerNodes);

        // Assign first node to alive list.
        tempList.remove(0);
        aliveList.add(bigListServerNodes.get(0));

        Collections.shuffle(tempList);
        int numKill = SHUTDOWN_NODE_COUNT; //(int) Math.ceil(bigListServerNodes.size() * PERCENT_SHUTDOWN);
        System.out.println("Rolling crash test: to shutdown - " + numKill + " nodes.");

        if (bigListServerNodes.size() < (numKill + 1)) {
            System.err.println("Error: need at least " + (numKill + 1) + " nodes to run the crash test. Aborting.");
            return;
        }

        // Assign nodes to crash or alive lists.
        for (ServerNode node : tempList) {
            if (numKill > 0) {
                crashList.add(node);
                numKill--;
            } else {
                aliveList.add(node);
            }
        }

        // Execute first crash test on alive-list and crash-list of nodes.
        Tests.testUtilStartTimer();
        StringBuilder resultString = new StringBuilder();
        TestStatus crashStatus = TestsPool.testRollingCrashVerifyWithExtraLoad(aliveList, crashList, CAPACITY_INPUT_SIZE, CAPACITY_INPUT, CAPA_TEST_TIMER, LOWERBOUND_ALLOWED, UPPERBOUND_ALLOWED,
                resultString, TestsConfig.getClientsCount(), PAUSE_SECONDS, resultMap, MAX_KEYS_SC, secret);
        if (crashStatus != TestStatus.TEST_PASSED) {
            System.err.println("Crash test failed. Aborting.");
            return;
        }
        System.out.println(crashStatus);
        resultMap.add(new KVSResultField("Test Rolling Crash; Key Recovery", "PASSED"));
        resultMap.add(new KVSResultField("Test Rolling Crash; % Keys failed", resultString.toString()));

        // Wipe-out.
        if (TestsPool.testWipeoutAll(aliveList) != TestStatus.TEST_PASSED) {
            System.err.println("Unsuccessful wipe-out. Aborting.");
            return;
        }
        Tests.testUtilElapsedTime();

        // Second set of Single server performance test (first node).
        for (int clients : CLIENT_TYPES_SINGLE_SERVER) {
            Tests.testUtilStartTimer();
            KVSTestStats testSingleServerStats = new KVSTestStats();
            TestsPool.testSinglePerformanceTimed(aliveList.get(0), clients, testSingleServerStats, PERF_TEST_TIMER);
            if (testSingleServerStats.getResponseCount() != 0) {
                testSingleServerStats.printAggregateSummaryStats(resultMap, "Stage 2 Single Front-End");
            } else {
                System.err.println("No response statistics collected!");
            }

            // Wipe-out.
            if (TestsPool.testWipeoutAll(aliveList) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipe-out. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Second set of Distributed performance test (alive list).
        for (int clients : CLIENT_TYPES) {
            Tests.testUtilStartTimer();
            TestStatus status;
            KVSTestStats testStats = new KVSTestStats();
            if ((clients < 1024) || (clients == 1024 && !exclude1024) || (clients == 2048 && !exclude2048)) {
                status = TestsPool.testDistributedPerformanceTimed(aliveList, clients, testStats, PERF_TEST_TIMER);
            } else {
                status = TestStatus.TEST_PASSED;
            }

            System.out.println(status);

            String s2 = "NA";
            if (clients >= 32) {
                s2 = OSUtils.loadavg();
            }

            if (testStats.getResponseCount() != 0) {
                testStats.printAggregateSummaryStats(resultMap, "Stage 2 Random Front-End", s2, clients);
            } else {
                System.err.println("No response statistics collected!");
            }

            if (status != TestStatus.TEST_PASSED) {
                System.err.println("Test did not pass. Aborting. Maximum Clients = " + clients);
                return;
            }

            // Wipe-out.
            if (TestsPool.testWipeoutAll(aliveList) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipe-out. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Ensure shutdown obeyed.
//        Tests.testUtilStartTimer();
//        if (TestsPool.testIfAllDeadWithPOP(crashList, secret) != TestStatus.TEST_PASSED) {
//            System.out.println("Error: Server(s) are still up after being issued shutdown command!");
//            return;
//        }
//        Tests.testUtilElapsedTime();

        // Report how many nodes are still up
        resultMap.add(new KVSResultField("Number of alive nodes after tests are completed", Integer.toString(TestsPool.testDeadOrAlive(bigListServerNodes))));

        System.out.println();
        String code = TestUtils.printResultMapSummary(resultMap);
        resultMap.add(new KVSResultField("Verification Code", code));

        System.out.println("Completed Milestone 3 Smoke Tests.");
    }

    static void runM3(String[] args) {
        try {
            String logFileName = "milestone3.log";
            PrintStream stream = new PrintStream(new File(logFileName));
            System.setOut(stream);
            System.setErr(stream);

            System.out.println("CPEN 431 Test Client");
            System.out.println("Time: " + new Date().toString());
            System.out.println("Build Date: " + getClassBuildTime());
            System.out.println("Java process ID: " + OSUtils.getPIDOfJavaExecutable());

            long startTime = System.currentTimeMillis();

            // args[0] is the file path for the list of servers
            // args[1] is the student ID

            System.out.println("Running tests!");
            executeM3SmokeTests(args[0], "0", true, true);

            // If you want to execute extended tests then you should
            // run the client from a GCP e2-standard-16 instance at least.
            // The comment out the line above and use the following:
            // executeM3SmokeTests(args[0], "0", false, false);

            System.out.println("Testing completed in " + (System.currentTimeMillis() - startTime) / (double) 1000 + "s");

            stream.flush();
            stream.close();

            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));

            byte[] logFileBytes = Files.readAllBytes(Paths.get(logFileName));
            byte[] logFileHash = MessageDigest.getInstance("MD5").digest(logFileBytes);

            String logFileHashHex = DatatypeConverter.printHexBinary(logFileHash);

            System.out.println("Please include the following verification code in your readme.md file!");
            System.out.println("Verification Code: "
                    + logFileHashHex.charAt(21)
                    + logFileHashHex.charAt(28)
                    + logFileHashHex.charAt(30)
                    + logFileHashHex.charAt(15)
                    + logFileHashHex.charAt(10)
                    + logFileHashHex.charAt(17)
                    + logFileHashHex.charAt(4));

        } catch (Exception e) {
            System.err.println("Could not complete the tests!");
            System.err.println("Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws IOException {
        runM3(args);
    }
}
