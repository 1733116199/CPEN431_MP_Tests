package cpen431.mp.Tests;

import cpen431.mp.Statistics.KVSTestStats;
import cpen431.mp.Utilities.OSUtils;
import cpen431.mp.Utilities.ServerNode;
import cpen431.mp.KeyValue.KVSResultField;
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
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public class TestDriver {

    // static methods

    /**
     * @param bigListFileName
     * @throws IOException
     */

    private static final String resultServiceURL = "http://35.165.135.83:43107";
    private static final String gcInstanceMetadataURL = "http://169.254.169.254/latest/meta-data/";

    public static String getGCInstanceType() {
        String instanceType = "NA";

        try {
            URL url = new URL(gcInstanceMetadataURL + "instance-type");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            int responseCode = con.getResponseCode();

            if (responseCode != HttpURLConnection.HTTP_OK) {
                return instanceType;
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            con.disconnect();

            instanceType = response.toString();
        } catch (Exception ex) {
            instanceType = "NA";
        }

        return instanceType.trim();
    }

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

    public static void executeTestsM2(String bigListFileName, String secret, boolean excludeTest) throws IOException {
        final long PERF_TEST_TIMER = 60 * 1000;
        final long CAPA_TEST_TIMER = 600 * 1000;
        final int MAX_PROTOCOL_VALUE_BYTES = 1000;
        final int[] CLIENT_TYPES_SINGLE_SERVER = new int[]{1, 16, 32};
        final int[] CLIENT_TYPES = new int[]{1, 16, 32, 64, 128, 256};
        final int MIN_NODE_COUNT = 3;
        final int SUBMIT_NODE_COUNT = 3;
        final int SHUTDOWN_NODE_COUNT = 3;
        final int TIME_BETWEEN_CRASHES = 120;

        //final double PERCENT_SHUTDOWN = 0.12;
        final double LOWERBOUND_ALLOWED = 0.0;
        final double UPPERBOUND_ALLOWED = 0.8;
        final long MAX_CAPACITY_MEM_BYTES = 10 * 1000 * 1000;
        final int CAPACITY_INPUT = (int) (MAX_CAPACITY_MEM_BYTES / MAX_PROTOCOL_VALUE_BYTES);

        System.out.println("[Milestone2 Tests]");
        System.out.println("Secret: " + secret);
        System.out.println("Exclude 256 clients tests: " + excludeTest);

        System.out.println("Checking if the test client is running on GC");
        String instanceType = getGCInstanceType();
        if (instanceType.equals("NA")) {
            System.err.println("Unable to determine instance type.");
            System.err.println("Are you sure you are running the test client on an GC instance?");
        } else {
            System.out.println("Detected instance type: " + instanceType);
        }
        System.out.println();

        ArrayList<ServerNode> bigListServerNodes = TestUtils.getServerNodes(bigListFileName);
        if (bigListServerNodes == null)
            return;

        System.out.println("The deployment has " + bigListServerNodes.size() + " server nodes.");

        if (bigListServerNodes.size() < MIN_NODE_COUNT) {
            System.err.println("Error! Need to run on at least " + MIN_NODE_COUNT + " nodes to execute the tests.");
            System.err.println("A7 test application targets " + SUBMIT_NODE_COUNT + "+ nodes.");
            return;
        }

        if ((bigListServerNodes.size() >= MIN_NODE_COUNT) && (bigListServerNodes.size() <= SUBMIT_NODE_COUNT)) {
            System.err.println("Warning! It is recommended to run on " + SUBMIT_NODE_COUNT + "+ nodes to submit.");
            System.err.println("Milestone2 test application targets " + SUBMIT_NODE_COUNT + "+ nodes.");
        }

        // Test to check if results server is up and the provided secret is valid
        if (!secret.equals("0")) {
            if (postSecretJSON(secret)) {
                System.out.println("Results server is up. Secret is valid.");
            } else {
                System.err.println("Either the results server is down or secret is invalid. Aborting!");
                return;
            }
        }

        // Create a list with the relevant results (to be submitted to the results collection server)
        ArrayList<KVSResultField> resultMap = new ArrayList<>();
        resultMap.add(new KVSResultField("Test Start Time", new Date().toString()));
        resultMap.add(new KVSResultField("Client GC Instance Type", instanceType));
        resultMap.add(new KVSResultField("Client Local IP", InetAddress.getLocalHost().toString()));
        resultMap.add(new KVSResultField("Client Local Hostname", InetAddress.getLocalHost().getHostName()));
        resultMap.add(new KVSResultField("Server List", getFileName(bigListServerNodes)));
        resultMap.add(new KVSResultField("Server Count", Integer.toString(bigListServerNodes.size())));

        // If user intends to submit results, ensure we have at least the specified node count.
        if (!secret.equals("0")) {
            if (bigListServerNodes.size() < SUBMIT_NODE_COUNT) {
                System.err.println("Error: need to run on at least " + SUBMIT_NODE_COUNT + " nodes to submit.");
                return;
            }
        }

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
            if ((clients < 256) || (clients == 256 && !excludeTest)) {
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
                System.err.println("No response statistics collected for "+ clients+" clients!");
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
        TestStatus capStatus = TestsPool.testInsertCapacity(bigListServerNodes, MAX_PROTOCOL_VALUE_BYTES,
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
        System.out.println("First crash test: to shutdown - " + numKill + " nodes.");

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
        TestStatus crashStatus = TestsPool.testCrashVerify(aliveList, crashList, MAX_PROTOCOL_VALUE_BYTES, CAPACITY_INPUT, CAPA_TEST_TIMER, LOWERBOUND_ALLOWED, UPPERBOUND_ALLOWED,
                resultString, TestsConfig.getClientsCount());
        if (crashStatus != TestStatus.TEST_PASSED) {
            System.err.println("Crash test failed. Aborting.");
            return;
        }
        System.out.println(crashStatus);
        resultMap.add(new KVSResultField("Test Crash 1; Key Recovery", "PASSED"));
        resultMap.add(new KVSResultField("Test Crash 1; % Keys failed", resultString.toString()));

        // Important: no wipe-out here.

        Tests.pause(TIME_BETWEEN_CRASHES); // wait for two minutes

        // Second Crash test -- first, set up list of nodes to crash.
        ArrayList<ServerNode> crashListTwo = new ArrayList<ServerNode>();
        ArrayList<ServerNode> aliveListTwo = new ArrayList<ServerNode>();
        ArrayList<ServerNode> tempListTwo = new ArrayList<ServerNode>(aliveList);

        // Assign first node to alive list.
        tempListTwo.remove(0);
        aliveListTwo.add(aliveList.get(0));

        Collections.shuffle(tempListTwo);
        numKill = SHUTDOWN_NODE_COUNT; //(int) Math.ceil(bigListServerNodes.size() * PERCENT_SHUTDOWN);
        System.out.println("Second crash test: to shutdown - " + numKill + " nodes.");

        // Assign nodes to crash or alive lists.
        for (ServerNode node : tempListTwo) {
            if (numKill > 0) {
                crashListTwo.add(node);
                numKill--;
            } else {
                aliveListTwo.add(node);
            }
        }
        Tests.testUtilStartTimer();
        StringBuilder resultStringTwo = new StringBuilder();
        TestStatus crashStatusTwo = TestsPool.testCrashVerify(aliveListTwo, crashListTwo, MAX_PROTOCOL_VALUE_BYTES, CAPACITY_INPUT, CAPA_TEST_TIMER, LOWERBOUND_ALLOWED, UPPERBOUND_ALLOWED,
                resultStringTwo, TestsConfig.getClientsCount());
        if (crashStatusTwo != TestStatus.TEST_PASSED) {
            System.err.println("Crash test failed. Aborting.");
            return;
        }
        System.out.println(crashStatusTwo);
        resultMap.add(new KVSResultField("Test Crash 2; Key Recovery", "PASSED"));
        resultMap.add(new KVSResultField("Test Crash 2; % Keys failed", resultStringTwo.toString()));

        // Wipe-out.
        if (TestsPool.testWipeoutAll(aliveListTwo) != TestStatus.TEST_PASSED) {
            System.err.println("Unsuccessful wipe-out. Aborting.");
            return;
        }
        Tests.testUtilElapsedTime();

        // Second set of Single server performance test (first node).
        for (int clients : CLIENT_TYPES_SINGLE_SERVER) {
            Tests.testUtilStartTimer();
            KVSTestStats testSingleServerStats = new KVSTestStats();
            TestsPool.testSinglePerformanceTimed(aliveListTwo.get(0), clients, testSingleServerStats, PERF_TEST_TIMER);
            if (testSingleServerStats.getResponseCount() != 0) {
                testSingleServerStats.printAggregateSummaryStats(resultMap, "Stage 2 Single Front-End");
            } else {
                System.err.println("No response statistics collected!");
            }

            // Wipe-out.
            if (TestsPool.testWipeoutAll(aliveListTwo) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipeout. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Second set of Distributed performance test (alive list).
        for (int clients : CLIENT_TYPES) {
            Tests.testUtilStartTimer();
            TestStatus status;
            KVSTestStats testStats = new KVSTestStats();
            if ((clients < 256) || (clients == 256 && !excludeTest)) {
                status = TestsPool.testDistributedPerformanceTimed(aliveListTwo, clients, testStats, PERF_TEST_TIMER);
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
            if (TestsPool.testWipeoutAll(aliveListTwo) != TestStatus.TEST_PASSED) {
                System.err.println("Unsuccessful wipe-out. Aborting.");
                return;
            }
            Tests.testUtilElapsedTime();
        }

        // Ensure shutdown obeyed.
        Tests.testUtilStartTimer();
        if (TestsPool.testIfAllDead(crashList) != TestStatus.TEST_PASSED) {
            System.out.println("Error: Server(s) are still up after being issued shutdown command!");
            //return;
        }
        Tests.testUtilElapsedTime();

        System.out.println("Completed Milestone2 Tests.");
    }

    public static void executeTestsM1(String bigListFileName, String secret, boolean excludeTest) throws IOException {
        final long PERF_TEST_TIMER = 60 * 1000;
        final long CAPA_TEST_TIMER = 600 * 1000;
        final int MAX_PROTOCOL_VALUE_BYTES = 1000;
        final int[] CLIENT_TYPES_SINGLE_SERVER = new int[]{1, 16, 32};
        final int[] CLIENT_TYPES = new int[]{1, 16, 32, 64, 128, 256};
        final int MIN_NODE_COUNT = 3;
        final int SUBMIT_NODE_COUNT = 3;
        final int SHUTDOWN_NODE_COUNT = 3;
        final int TIME_BETWEEN_CRASHES = 120;

        //final double PERCENT_SHUTDOWN = 0.12;
        final double LOWERBOUND_ALLOWED = 0.0;
        final double UPPERBOUND_ALLOWED = 0.8;
        final long MAX_CAPACITY_MEM_BYTES = 10 * 1000 * 1000;
        final int CAPACITY_INPUT = (int) (MAX_CAPACITY_MEM_BYTES / MAX_PROTOCOL_VALUE_BYTES);

        System.out.println("[Milestone1 Tests]");
        System.out.println("Secret: " + secret);
        System.out.println("Exclude 256 clients tests: " + excludeTest);

        System.out.println("Checking if the test client is running on GC");
        String instanceType = getGCInstanceType();
        if (instanceType.equals("NA")) {
            System.err.println("Unable to determine instance type.");
            System.err.println("Are you sure you are running the test client on an GC instance?");
        } else {
            System.out.println("Detected instance type: " + instanceType);
        }
        System.out.println();

        if (!secret.equals("0") && !instanceType.equals("t2.micro")) {
            System.err.println("Error! Need to run the test client on a t2.micro EC2 instance to be able to submit results!");
            return;
        }

        ArrayList<ServerNode> bigListServerNodes = TestUtils.getServerNodes(bigListFileName);
        if (bigListServerNodes == null)
            return;

        System.out.println("The deployment has " + bigListServerNodes.size() + " server nodes.");

        if (bigListServerNodes.size() < MIN_NODE_COUNT) {
            System.err.println("Error! Need to run on at least " + MIN_NODE_COUNT + " nodes to execute the tests.");
            System.err.println("Milestone1 test application targets " + SUBMIT_NODE_COUNT + "+ nodes.");
            return;
        }

        if ((bigListServerNodes.size() >= MIN_NODE_COUNT) && (bigListServerNodes.size() <= SUBMIT_NODE_COUNT)) {
            System.err.println("Warning! It is recommended to run on " + SUBMIT_NODE_COUNT + "+ nodes to submit.");
            System.err.println("Milestone1 test application targets " + SUBMIT_NODE_COUNT + "+ nodes.");
        }

        // Test to check if results server is up and the provided secret is valid
        if (!secret.equals("0")) {
            if (postSecretJSON(secret)) {
                System.out.println("Results server is up. Secret is valid.");
            } else {
                System.err.println("Either the results server is down or secret is invalid. Aborting!");
                return;
            }
        }

        // Create a list with the relevant results (to be submitted to the results collection server)
        ArrayList<KVSResultField> resultMap = new ArrayList<>();
        resultMap.add(new KVSResultField("Test Start Time", new Date().toString()));
        resultMap.add(new KVSResultField("Client GC Instance Type", instanceType));
        resultMap.add(new KVSResultField("Client Local IP", InetAddress.getLocalHost().toString()));
        resultMap.add(new KVSResultField("Client Local Hostname", InetAddress.getLocalHost().getHostName()));
        resultMap.add(new KVSResultField("Server List", getFileName(bigListServerNodes)));
        resultMap.add(new KVSResultField("Server Count", Integer.toString(bigListServerNodes.size())));

        // If user intends to submit results, ensure we have at least the specified node count.
        if (!secret.equals("0")) {
            if (bigListServerNodes.size() < SUBMIT_NODE_COUNT) {
                System.err.println("Error: need to run on at least " + SUBMIT_NODE_COUNT + " nodes to submit.");
                return;
            }
        }

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

        // Ensure basic functionality (single node).
        Tests.testUtilStartTimer();
        for (int valueLength : new int[]{8, 32, 2048, 10000}) {
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

        // Ensure basic functionality (distributed).
        Tests.testUtilStartTimer();
        for (int valueLength : new int[]{8, 32, 2048, 10000}) {
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

        // Get process IDs from all the nodes.
        if (TestsPool.testGetPIDAll(bigListServerNodes) != TestStatus.TEST_PASSED) {
            System.err.println("Process ID retrieval was unsuccessful. Aborting.");
            return;
        }

        // Report how many nodes are still up
        resultMap.add(new KVSResultField("Number of alive nodes after tests are completed", Integer.toString(TestsPool.testDeadOrAlive(bigListServerNodes))));

        System.out.println();
        String code = TestUtils.printResultMapSummary(resultMap);
        resultMap.add(new KVSResultField("Verification Code", code));

        // Test to check if results server is up and the provided secret is valid
        if (!secret.equals("0")) {
            if (postResultsJSON(secret, resultMap)) {
                System.out.println("Results server is up. Secret is valid.");
                System.out.println("Results successfully submitted.");
            } else {
                System.err.println("Either the results server is down or secret is invalid.");
                System.err.println("Failed to submit results!");
            }
        }

        System.out.println("Completed Milestone1 Tests.");
    }

    static void runTests(String[] args){
        try {
            String logFileName = "results.log";
            PrintStream stream = new PrintStream(new File(logFileName));
            System.setOut(stream);
            System.setErr(stream);

            System.out.println("CPEN 431 Test Client");
            System.out.println("Time: " + new Date().toString());
            System.out.println("Build Date: " + getClassBuildTime());
            System.out.println("Java process ID: " + OSUtils.getPIDOfJavaExecutable());

            long startTime = System.currentTimeMillis();

            // args[1] is the file path for the list of servers
            // args[2] is the student ID

            if (args.length == 3) {
                if(args[0].equals("1")){
                    System.out.println("Running test in local mode! Milestone1.");
                    executeTestsM1(args[1], "0", Boolean.parseBoolean(args[2].toLowerCase().replaceAll("\\s+", "")));
                } else {
                    System.out.println("Running test in local mode! Milestone2.");
                    executeTestsM2(args[1], "0", Boolean.parseBoolean(args[2].toLowerCase().replaceAll("\\s+", "")));
                }
            }
            else if (args.length == 4) {
                if(args[0].equals("1")) {
                    System.out.println("Milestone1, Running test in submit mode using secret code: " + args[3] + "!");
                    executeTestsM1(args[1], args[3], false);
                }else{
                    System.out.println("Milestone2, Running test in submit mode using secret code: " + args[3] + "!");
                    executeTestsM2(args[1], args[3], false);
                }
            }
            else {
                System.err.println("Missing Argument: \n" +
                        "1) Test mode arguments: server-list-file-path exclude-256clients-test \n" +
                        "2) Submit mode arguments: server-list-file-path exclude-256clients-test secret-key");
                return;
            }

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
        }
    }

    public static void main(String args[]) throws IOException {
        runTests(args);
    }
}
