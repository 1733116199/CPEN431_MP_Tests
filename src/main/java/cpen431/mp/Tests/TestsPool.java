package cpen431.mp.Tests;

import cpen431.mp.KeyValue.KVSResponse;
import cpen431.mp.ProtocolBuffers.KeyValueRequest;
import cpen431.mp.ProtocolBuffers.KeyValueResponse;
import cpen431.mp.RequestReply.RRClientAPI;
import cpen431.mp.Statistics.KVSClientLog;
import cpen431.mp.Statistics.KVSTestStats;
import cpen431.mp.Utilities.ServerNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import cpen431.mp.KeyValue.KVSClientAPI;
import cpen431.mp.KeyValue.KVSRequestType;
import cpen431.mp.KeyValue.KVSResponseStatus;
import cpen431.mp.KeyValue.KVSResultField;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static cpen431.mp.Tests.Tests.getRandomInteger;

public class TestsPool {
    private static String[]  failedServerNodes = TestsConfig.getFailedServerNodes();
    private static Random RAND = new Random();
    private static int MAX_REPEAT = 10;


    public static TestStatus testGetPIDLite(final ArrayList<ServerNode> serverNode) {
        if (serverNode.size() > 1) {
            System.err.println("Error: More than one element.");
            return TestStatus.TEST_FAILED;
        }
        boolean testFailed = false;
        for (int i = 0; i < TestsConfig.getMaxRepeatLite(); i++) {
            KVSResponse getPID = KVSClientAPI.getPID(serverNode);
            if (getPID.status == KVSResponseStatus.TIMEOUT) {
                serverNode.get(0).setProcessID(-2);
                continue;
            }
            if (getPID.status != KVSResponseStatus.SUCCESS) {
                serverNode.get(0).setProcessID(-1);
                testFailed = true;
                continue;
            }
            int processID = getPID.pid;
            serverNode.get(0).setProcessID(processID);
            return TestStatus.TEST_PASSED;
        }
        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            return TestStatus.TEST_UNDECIDED;
        }
    }

    public static TestStatus testGetPIDAll(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Sending GETPID to all servers... ");
        System.out.flush();
        TestsPool.failedServerNodes = new String[serverNodes.size()];
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    ArrayList<ServerNode> single = new ArrayList<>();
                    single.add(serverNodes.get(serverNodeIndex));
                    if (testGetPIDLite(single) != TestStatus.TEST_PASSED) {
                        TestsPool.failedServerNodes[serverNodeIndex] = serverNodes.get(serverNodeIndex).getHostName();
                    } else {
                        TestsPool.failedServerNodes[serverNodeIndex] = "NA";
                    }
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        boolean testFailed = false;

        for (int i = 0; i < serverNodes.size(); i++) {
            if (!TestsPool.failedServerNodes[i].equalsIgnoreCase("NA")) {
                System.out.println(">>> Server node " + serverNodes.get(i).getHostName() + " failed to return process ID.");
                testFailed = true;
            } else {
                System.out.println(">>> Server node " + serverNodes.get(i).getHostName() + " process ID " + serverNodes.get(i).getProcessID() + ".");
            }
        }

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");

        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            System.out.println("[OK]");
            return TestStatus.TEST_PASSED;
        }
    }

    public static TestStatus testWipeoutLite(final ArrayList<ServerNode> serverNode) {
        boolean testFailed = false;
        for (int i = 0; i < TestsConfig.getMaxRepeatLite(); i++) {
            KVSResponse wipeout = KVSClientAPI.wipeout(serverNode);
            if (wipeout.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (wipeout.status != KVSResponseStatus.SUCCESS) {
                testFailed = true;
                continue;
            }
            return TestStatus.TEST_PASSED;
        }
        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            return TestStatus.TEST_UNDECIDED;
        }
    }

    public static TestStatus testWipeoutAll(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Sending WIPE-OUT to all servers...");
        System.out.flush();
        TestsPool.failedServerNodes = new String[serverNodes.size()];
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    ArrayList<ServerNode> single = new ArrayList<>();
                    single.add(serverNodes.get(serverNodeIndex));
                    if (testWipeoutLite(single) != TestStatus.TEST_PASSED) {
                        TestsPool.failedServerNodes[serverNodeIndex] = serverNodes.get(serverNodeIndex).getHostName();
                    } else {
                        TestsPool.failedServerNodes[serverNodeIndex] = "NA";
                    }
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        boolean testFailed = false;

        for (String fSN : TestsPool.failedServerNodes) {
            if (!fSN.equalsIgnoreCase("NA")) {
                System.out.println(">>> Server node " + fSN + " wipeout failed.");
                testFailed = true;
            }
        }

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");

        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            System.out.println("[OK]");
            return TestStatus.TEST_PASSED;
        }
    }

    public static void testShutdownList(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Sending SHUTDOWN to " + serverNodes.size() + " servers.");
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    ArrayList<ServerNode> single = new ArrayList<>();
                    single.add(serverNodes.get(serverNodeIndex));
                    KVSClientAPI.shutdown(single);
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");
    }

    // Test fails if the server does not reply correctly at least once for a small number of repeats
    public static TestStatus testIfSingleServerAlive(final ServerNode serverNode) {
        long st = System.currentTimeMillis();
        System.out.println("Checking if server is up... ");
        System.out.flush();

        if (testIsAlive(serverNode) != TestStatus.TEST_PASSED) {
            return TestStatus.TEST_FAILED;
        }
        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");
        System.out.println("[OK]");
        return TestStatus.TEST_PASSED;
    }

    public static TestStatus testIsAlive(final ServerNode serverNode) {
        ArrayList<ServerNode> single = new ArrayList<>();
        single.add(serverNode);

        System.out.println("[Test isAlive]");

        for (int i = 0; i < TestsConfig.getMaxRepeat(); i++) {
            KVSResponse response = KVSClientAPI.isAlive(single);

            if (response.status == KVSResponseStatus.TIMEOUT) {
                System.err.println("Request Timeout.");
                continue;
            } else if (response.status == KVSResponseStatus.SUCCESS) {
                return TestStatus.TEST_PASSED;
            } else {
                System.err.println("Response Error.");
                return TestStatus.TEST_FAILED;
            }
        }

        return TestStatus.TEST_UNDECIDED;
    }

    public static int testDeadOrAlive(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Checking which server(s) are up... ");
        System.out.flush();
        TestsPool.failedServerNodes = new String[serverNodes.size()];
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    if (testIsAlive(serverNodes.get(serverNodeIndex)) == TestStatus.TEST_PASSED) {
                        TestsPool.failedServerNodes[serverNodeIndex] = "Alive";
                    } else {
                        TestsPool.failedServerNodes[serverNodeIndex] = "Dead";
                    }
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int aliveCount = 0;
        int deadCount = 0;

        for (int i = 0; i < serverNodes.size(); i++) {
            if (TestsPool.failedServerNodes[i].equalsIgnoreCase("Alive")) {
                System.out.println(">>> Server node " + serverNodes.get(i).getHostName() + " is up.");
                aliveCount++;
            } else {
                System.out.println(">>> Server node " + serverNodes.get(i).getHostName() + " is down.");
                deadCount++;
            }
        }

        System.out.println("Out of " + serverNodes.size() + " servers, " + aliveCount + " are up and " + deadCount + " are down.");

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");

        return aliveCount;
    }

    public static TestStatus testIfAllAlive(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Checking if server(s) are up... ");
        System.out.flush();
        TestsPool.failedServerNodes = new String[serverNodes.size()];
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    if (testIfSingleServerAlive(serverNodes.get(serverNodeIndex)) != TestStatus.TEST_PASSED) {
                        TestsPool.failedServerNodes[serverNodeIndex] = serverNodes.get(serverNodeIndex).getHostName();
                    } else {
                        TestsPool.failedServerNodes[serverNodeIndex] = "NA";
                    }
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        boolean testFailed = false;

        for (String fSN : TestsPool.failedServerNodes) {
            if (!fSN.equalsIgnoreCase("NA")) {
                System.out.println(">>> Server node " + fSN + " is not behaving as expected.");
                testFailed = true;
            }
        }

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");

        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            System.out.println("[OK]");
            return TestStatus.TEST_PASSED;
        }
    }

    public static TestStatus testIfAllDead(final ArrayList<ServerNode> serverNodes) {
        long st = System.currentTimeMillis();
        System.out.println("Checking if server(s) are up... ");
        System.out.flush();
        TestsPool.failedServerNodes = new String[serverNodes.size()];
        Thread[] threads = new Thread[serverNodes.size()];
        for (int i = 0; i < serverNodes.size(); i++) {
            final int serverNodeIndex = i;
            threads[i] = new Thread() {
                public void run() {
                    if (testIsAlive(serverNodes.get(serverNodeIndex)) == TestStatus.TEST_PASSED) {
                        TestsPool.failedServerNodes[serverNodeIndex] = serverNodes.get(serverNodeIndex).getHostName();
                    } else {
                        TestsPool.failedServerNodes[serverNodeIndex] = "NA";
                    }
                }
            };
            threads[i].start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        boolean testFailed = false;

        for (String fSN : TestsPool.failedServerNodes) {
            if (!fSN.equalsIgnoreCase("NA")) {
                System.out.println(">>> Server node " + fSN + " is not down as expected.");
                testFailed = true;
            }
        }

        double et = (double) (System.currentTimeMillis() - st) / 1000.0;
        Tests.printMessage("... Completed in " + et + " seconds");

        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            System.out.println("[OK]");
            return TestStatus.TEST_PASSED;
        }
    }

    // Attempts to test the at-most-once policy.
    // PUT -> REM -> REM (with same UID thus should succeed not fail)
    // The test attempts multiple times before giving up.
    // Note that the test is implemented using the low-level API calls
    public static TestStatus testAtMostOncePolicy(final ArrayList<ServerNode> serverNodes) {

        System.out.println("[ TEST At-Most-Once Client Policy (PUT -> REM -> REM) ]");

        if (KVSClientAPI.wipeout(serverNodes).status != KVSResponseStatus.SUCCESS) {
            System.err.println("Test Undecided. Unable to wipe-out KVS. Server has failed.");
            return TestStatus.TEST_UNDECIDED;
        }

        for (int i = 0; i < TestsConfig.getMaxRepeat(); i++) {
            byte[] key = new byte[32];
            byte[] value = new byte[32];

            TestsConfig.getRAND().nextBytes(key);
            TestsConfig.getRAND().nextBytes(value);

            // Pick a random server node
            ServerNode randomServer = serverNodes.get(getRandomInteger(0, serverNodes.size() - 1,
                    TestsConfig.getRAND()));
            ArrayList<ServerNode> randomServerList = new ArrayList<>();
            randomServerList.add(randomServer);

            // Step 1: PUT Request
            KVSResponseStatus putStatus = KVSClientAPI.put(randomServerList, key, value).status;
            if (putStatus == KVSResponseStatus.TIMEOUT) {
                System.err.println("PUT timeout!");
                continue;
            } else if (putStatus != KVSResponseStatus.SUCCESS){
                System.err.println("PUT was not successful!");
                continue;
            }

            // Generate the REM application payload
            byte[] kvRequestPayloadRem = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(3)
                    .setKey(ByteString.copyFrom(key))
                    .build().toByteArray();

            // Create a Request/Reply Client
            RRClientAPI rrClient = new RRClientAPI(randomServer.getHostName(), randomServer.getPortNumber());

            // Step 2: REM Request
            byte[] recvRem1 = rrClient.sendAndReceive(kvRequestPayloadRem);
            byte[] msgIDRem = rrClient.getRequestMessageID();

            if (rrClient.isTimeout()) {
                System.err.println("First REM timeout!");
                continue;
            }

            // Verify that the request was successful
            try {
                int errCode = KeyValueResponse.KVResponse.parseFrom(recvRem1).getErrCode();
                if(errCode != 0) {
                    System.err.println("First REM was not successful!");
                    continue;
                }
            } catch (InvalidProtocolBufferException e) {
                System.err.println("Unable to parse response for first REM!");
                continue;
            }

            // Step 3: REM Request (same unique message ID as the previous REM command)
            byte[] recvRem2 = rrClient.sendAndReceive(kvRequestPayloadRem, msgIDRem);

            if (rrClient.isTimeout()) {
                System.err.println("Second REM timeout!");
                continue;
            }

            // Close the request/reply client
            rrClient.close();

            // Verify that the request was successful
            try {
                int errCode = KeyValueResponse.KVResponse.parseFrom(recvRem2).getErrCode();
                if(errCode == 0) {
                    return TestStatus.TEST_PASSED;
                } else if (errCode == 1) {
                    System.err.println("Server responded with no key error for the second REM request!");
                    return TestStatus.TEST_FAILED;
                } else {
                    System.err.println("Unexpected error code for the second REM request!");
                    continue;
                }
            } catch (InvalidProtocolBufferException e) {
                System.err.println("Unable to parse response for the second REM!");
                continue;
            }
        }

        return TestStatus.TEST_UNDECIDED;
    }


    // Attempt a basic PUT -> GET -> REM -> REM -> GET operation using a random (key, value) with the specified value length (for a single server)
// test fails if the server does not reply correctly at least once for a small number of repeats
    private static TestStatus testBasic(final ArrayList<ServerNode> serverNodes, int valueLength) {
        byte[] key = new byte[32];
        byte[] value = new byte[valueLength];

        TestsConfig.getRAND().nextBytes(key);
        TestsConfig.getRAND().nextBytes(value);

        for (int i = 0; i < TestsConfig.getMaxRepeat(); i++) {

// Issue put.
            KVSResponse putResponse = KVSClientAPI.put(serverNodes, key, value);
            if (putResponse.status != KVSResponseStatus.SUCCESS) {
                continue;
            }

// Issue get.
            KVSResponse getResponse = KVSClientAPI.get(serverNodes, key);
            if (getResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (getResponse.status != KVSResponseStatus.SUCCESS) {
                System.err.println("GET failed!");
                return TestStatus.TEST_FAILED;
            }
            if (!Arrays.equals(getResponse.value, value)) {
                System.err.println("Logic error: GET Value != PUT Value.");
                System.err.println("Put:" + value + ", Get:" + getResponse.value);
                return TestStatus.TEST_FAILED;
            }

// Issue remove 1.
            KVSResponse remResponse = KVSClientAPI.remove(serverNodes, key);
            if (remResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (remResponse.status != KVSResponseStatus.SUCCESS) {
                System.err.println("REM1 failed!");
                return TestStatus.TEST_FAILED;
            }
// Issue remove 2.
            KVSResponse remTwoResponse = KVSClientAPI.remove(serverNodes, key);
            if (remTwoResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (remTwoResponse.status == KVSResponseStatus.SUCCESS) {
                System.err.println("Logic error; successful REM after REM.");
                return TestStatus.TEST_FAILED;
            }

// Issue get 2.
            KVSResponse getTwoResponse = KVSClientAPI.get(serverNodes, key);
            if (getTwoResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (getTwoResponse.status == KVSResponseStatus.SUCCESS) {
                System.err.println("Logic error; successful GET after REM.");
                return TestStatus.TEST_FAILED;
            }
            return TestStatus.TEST_PASSED;
        }
        return TestStatus.TEST_UNDECIDED;
    }

    private static TestStatus testBasicLite(final ArrayList<ServerNode> serverNode, int valueLength) {
        byte[] key = new byte[32];
        byte[] value = new byte[valueLength];

        TestsConfig.getRAND().nextBytes(key);
        TestsConfig.getRAND().nextBytes(value);

        boolean testFailed = false;

        for (int i = 0; i < TestsConfig.getMaxRepeatLite(); i++) {
            // Issue put.
            KVSResponse putResponse = KVSClientAPI.put(serverNode, key, value);
            if (putResponse.status != KVSResponseStatus.SUCCESS) {
                continue;
            }

            // Issue get.
            KVSResponse getResponse = KVSClientAPI.get(serverNode, key);
            if (getResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (getResponse.status != KVSResponseStatus.SUCCESS) {
                //System.err.println("GET failed!");
                //return TestStatus.TEST_FAILED;
                testFailed = true;
                continue;
            }
            if (!Arrays.equals(getResponse.value, value)) {
                //System.err.println("Logic error: GET Value != PUT Value.");
                //System.err.println("Put:" + value + ", Get:" + getResponse.value);
                //return TestStatus.TEST_FAILED;
                testFailed = true;
                continue;
            }

            // Issue remove 1.
            KVSResponse remResponse = KVSClientAPI.remove(serverNode, key);
            if (remResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (remResponse.status != KVSResponseStatus.SUCCESS) {
                //System.err.println("REM1 failed!");
                //return TestStatus.TEST_FAILED;
                testFailed = true;
                continue;
            }
            // Issue remove 2.
            KVSResponse remTwoResponse = KVSClientAPI.remove(serverNode, key);
            if (remTwoResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (remTwoResponse.status == KVSResponseStatus.SUCCESS) {
                //System.err.println("Logic error; successful REM after REM.");
                //return TestStatus.TEST_FAILED;
                testFailed = true;
                continue;
            }

            // Issue get 2.
            KVSResponse getTwoResponse = KVSClientAPI.get(serverNode, key);
            if (getTwoResponse.status == KVSResponseStatus.TIMEOUT) {
                continue;
            }
            if (getTwoResponse.status == KVSResponseStatus.SUCCESS) {
                //System.err.println("Logic error; successful GET after REM.");
                //return TestStatus.TEST_FAILED;
                testFailed = true;
                continue;
            }
            return TestStatus.TEST_PASSED;
        }

        if (testFailed) {
            return TestStatus.TEST_FAILED;
        } else {
            return TestStatus.TEST_UNDECIDED;
        }
    }

    public static TestStatus testDistributedBasic(final ArrayList<ServerNode> serverNodes, int valueLength) {
        System.out.println("\n[ TEST Random Front-End Basic (PUT -> GET -> REM -> REM -> GET). Value Length = " + valueLength + " Bytes ]");
        return testBasic(serverNodes, valueLength);
    }

    public static TestStatus testSingleBasic(final ServerNode serverNode, int valueLength) {
        System.out.println("\n[ TEST Single Front-End Basic (PUT -> GET -> REM -> REM -> GET). Value Length = " + valueLength + " Bytes ]");
        ArrayList<ServerNode> serverNodes = new ArrayList<>();
        serverNodes.add(serverNode);
        return testBasic(serverNodes, valueLength);
    }

    // Tests performance for Front-Ends listed in serverNodes, and a given client count and time amount.
    private static TestStatus testPerformanceTimed(final ArrayList<ServerNode> serverNodes, final int clients,
                                                   KVSTestStats testStats, final long maxTimeMS) {
        TestStatus testStatus;

        // Prepare thread data structures
        final Boolean[] clientCorrectLogic = new Boolean[clients];
        @SuppressWarnings("unchecked")
        final ArrayList<KVSResponse> clientResponses[] = (ArrayList<KVSResponse>[]) new ArrayList[clients];
        for (int i = 0; i < clients; i++) {
            clientResponses[i] = new ArrayList<KVSResponse>();
            clientResponses[i].ensureCapacity(10000);
            clientCorrectLogic[i] = true;
        }

        final long test_multiplier = TestsConfig.getRAND().nextInt() + maxTimeMS;
        final int test_constant = TestsConfig.getRAND().nextInt();

        final long startTime = System.currentTimeMillis();

        // Prepare threads
        Thread[] thread = new Thread[clients];
        for (int i = 0; i < clients; i++) {
            final int tid = i;
            thread[i] = new Thread() {
                public void run() {
                    byte[] key = new byte[32];
                    ByteBuffer kbb = ByteBuffer.wrap(key);
                    kbb.putInt(0, tid);  // First int of the key is the TID.
                    kbb.putInt(4, test_constant);  // Second int is a test constant.
                    byte[] value = new byte[8];
                    ByteBuffer vbb = ByteBuffer.wrap(value);
                    final long endTime = maxTimeMS + startTime;
                    final long const_offset = (tid + 1) * test_multiplier + test_constant;

                    for (int i = 1; true; i++) {
                        kbb.putInt(8, i);  // Third int of the key is the iteration.
                        vbb.putLong(0, const_offset + i);  // value is a function of the constant(wrt tid) and the iteration.

                        clientResponses[tid].add(KVSClientAPI.put(serverNodes, key, value, tid));
                        clientResponses[tid].add(KVSClientAPI.get(serverNodes, key, tid));

                        if (System.currentTimeMillis() > endTime) {
//System.out.println("Test time limit reached: " + maxTimeMS / (double) 1000 + "s.");
//System.out.println("Test completed after " + i + " closed loops.");
                            return;
                        }
                    }
                }
            };
        }
// Start threads
        for (int i = 0; i < clients; i++) {
            thread[i].start();
        }

// Wait for threads to finish
        for (Thread t : thread) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify correctness of values.
        for (int tid = 0; tid < clients; tid++) {
            for (int i = 1; i < clientResponses[tid].size(); i += 2) {  // By twos (put and get)
                KVSResponse cr_put = clientResponses[tid].get(i - 1);
                KVSResponse cr_get = clientResponses[tid].get(i);
                if (cr_put.status == KVSResponseStatus.TIMEOUT) {
                // Ignore timed-out puts: but make corresponding GET a non-valid stat.
                    cr_get.validStat = false;
                    continue;
                }
                if (cr_put.status != KVSResponseStatus.SUCCESS) {
                    if (cr_put.status == KVSResponseStatus.ERROR_UID) {
                        System.err.println("Warning: PUT response received with an invalid message ID!");
                        cr_get.validStat = false;
                        continue;
                    } else {
                        System.err.println("Error: put fail!");
                        System.err.println("Status: " + cr_put.status);
                        System.err.println("Error Code: " + cr_put.errorCode);
                        clientCorrectLogic[tid] = false;
                        break;
                    }
                }
                if (cr_get.status == KVSResponseStatus.TIMEOUT) {
                    continue;
                }  // Ignore timed-out gets.
                if (cr_get.status != KVSResponseStatus.SUCCESS) {
                    if (cr_get.status == KVSResponseStatus.ERROR_UID) {
                        System.err.println("Warning: GET response received with an invalid message ID!");
                        continue;
                    } else {
                        System.err.println("Error: get fail!");
                        System.err.println("Status: " + cr_get.status);
                        System.err.println("Error Code: " + cr_get.errorCode);
                        clientCorrectLogic[tid] = false;
                        break;
                    }
                }
                int key = (i + 1) / 2;
                if (cr_get.value == null || cr_get.value.length == 0) {
                    System.err.println("Error: no (or zero length) value from get!");
                    clientCorrectLogic[tid] = false;
                    break;
                }
                ByteBuffer vbb = ByteBuffer.wrap(cr_get.value);
                long value = vbb.getLong(0);
                long expected = (tid + 1) * test_multiplier + test_constant + key;
                if (value != expected) {
                    System.err.println("Error: GET != PUT. Put: " + expected + " Get: " + value);
                    clientCorrectLogic[tid] = false;

// client logging
                    if (KVSClientLog.enableLogging && KVSClientLog.logError) {
                        KVSClientLog clientLog = KVSClientLog.kvsClientLogs[tid];
                        KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
                                Tests.toHumanReadableString(DatatypeConverter.printHexBinary((cr_get.requestUID == null ? "null".getBytes() : cr_get.requestUID))),
                                KVSRequestType.GET,
                                Tests.toHumanReadableString(DatatypeConverter.printHexBinary((cr_get.key == null ? "null".getBytes() : cr_get.key))),
                                Long.toHexString(expected),
                                Long.toHexString(value),
                                cr_get.errorCode,
                                KVSResponseStatus.WRONG_VALUE);
                        clientLog.messageExchangesError.add(me);
                    }

                    break;
                }
            }
        }

        for (int c = 0; c < clients; c++) {
            if (clientCorrectLogic[c] == false) {
                testStatus = TestStatus.TEST_FAILED;
                System.err.println("Test Failed. Logic Error.");
                System.err.println("Test Aborted!");
                return TestStatus.TEST_FAILED;
            }
        }

        if (clients == 64) {
            Tests.pause(10);
        } else if (clients == 128) {
            Tests.pause(20);
        } else if (clients == 256) {
            Tests.pause(30);
        }

// Test passed if distributed KVS is still operational
        switch (testIfAllAlive(serverNodes)) {
            case TEST_PASSED:
// System.out.println("Test Passed. KVS is ok.");
                testStatus = TestStatus.TEST_PASSED;
                break;
            case TEST_FAILED:
                if (TestsConfig.isStrictIsAlive()) {
                    System.err.println("Test Failed. KVS not responding correctly after test!");
                    testStatus = TestStatus.TEST_FAILED;
                } else {
                    System.err.println("Warning! Test Failed. KVS not responding correctly after test!");
                    testStatus = TestStatus.TEST_PASSED;
                }
                break;
            case TEST_UNDECIDED:
                if (TestsConfig.isStrictIsAlive()) {
                    System.err.println("Test Undecided. KVS timeout.");
                    testStatus = TestStatus.TEST_FAILED;
                } else {
                    System.err.println("Warning! Test Undecided. KVS timeout.");
                    testStatus = TestStatus.TEST_PASSED;
                }
                break;
            default:
                testStatus = TestStatus.TEST_UNDECIDED;
        }

// Prepare statistics
        testStats.setTestStatus(testStatus);
        testStats.setTestDuration(elapsedTime);
        for (int i = 0; i < clients; i++) {
            testStats.addResponses(i, clientResponses[i]);
        }

        return testStatus;
    }

    public static TestStatus testDistributedPerformanceTimed(final ArrayList<ServerNode> serverNodes,
                                                             final int clients, KVSTestStats testStats,
                                                             final long maxTimeMS) {
        System.out.println("\n[ TEST Random Front-End Performance (" + clients + " clients, " + maxTimeMS / (double) 1000 +
                " seconds): PUT -> GET (closed loop) ]");
        return testPerformanceTimed(serverNodes, clients, testStats, maxTimeMS);
    }

    public static TestStatus testSinglePerformanceTimed(final ServerNode serverNode,
                                                        final int clients, KVSTestStats testStats,
                                                        final long maxTimeMS) {
        System.out.println("\n[ TEST Single Front-End Performance (" + clients + " clients, " + maxTimeMS / (double) 1000 +
                " seconds): PUT -> GET (closed loop) ]");
        final ArrayList<ServerNode> serverNodes = new ArrayList<>();
        serverNodes.add(serverNode);
        return testPerformanceTimed(serverNodes, clients, testStats, maxTimeMS);
    }

    public static TestStatus testInsertCapacity(final ArrayList<ServerNode> serverNodes, final int valueSize,
                                                int uMaxPuts, final long maxTimeMS,
                                                ArrayList<KVSResultField> resultMap, final int CLIENTS) {
        // Setup the test and prepare the server.
        //final int CLIENTS = CLIENTS_COUNT;
        uMaxPuts = uMaxPuts / CLIENTS;
        final int fmaxPuts = uMaxPuts * CLIENTS;
        final byte[][] valueArray = new byte[fmaxPuts][valueSize];

        String size = new DecimalFormat("#0.00").format(fmaxPuts * valueSize / ((double) 1024 * 1024));
        resultMap.add(new KVSResultField("Insert Capacity (MiB)", size));
        System.out.println("\n[ TEST Capacity Insert (Value Size = " + valueSize +
                " bytes, Limit = " + fmaxPuts + " PUTs, ~" + size + " MiB) ]");


        long ttime = System.currentTimeMillis();
        // Issue all puts.
        System.out.println("Issuing puts... ");
        System.out.flush();
        final Boolean[] clientCorrectLogic = new Boolean[CLIENTS];
        final int[] clientTimeout = new int[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            clientTimeout[i] = 0;
            clientCorrectLogic[i] = true;
        }
// Prepare threads
        Thread[] thread = new Thread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int tid = i;
            thread[i] = new Thread() {
                public void run() {
                    final long pStartTime = System.currentTimeMillis();
// Divide keys into thread-space.
                    for (int key = fmaxPuts / CLIENTS * tid; key < fmaxPuts / CLIENTS * (tid + 1); key++) {
                        TestsConfig.getRAND().nextBytes(valueArray[key]);

                        KVSResponse putResponse = KVSClientAPI.put(serverNodes, KVSClientAPI.intToByte(key), valueArray[key], tid);

                        if (putResponse.status == KVSResponseStatus.TIMEOUT) {
                            clientTimeout[tid]++;
                            valueArray[key] = null;
                            continue;
                        } else if (putResponse.status != KVSResponseStatus.SUCCESS) {
                            System.err.println(putResponse.errorCode);
                            System.err.println("Received error code.");
                            clientCorrectLogic[tid] = false;
                            return;
                        }

                        if (maxTimeMS > 0 && System.currentTimeMillis() - pStartTime > maxTimeMS) {
                            System.err.println("Test is taking too long! Exceeded specified limit: " + maxTimeMS / (double) 1000 + "s");
                            System.err.println("Thread aborting after " + (key - (fmaxPuts / CLIENTS * tid)) + " puts (~" +
                                    ((key - (fmaxPuts / CLIENTS * tid)) * valueSize / ((double) 1024 * 1024)) + " MiB).");
                            clientCorrectLogic[tid] = false;
                            return;
                        }
                    }
                }
            };
        }
// Start threads
        for (int i = 0; i < CLIENTS; i++) {
            thread[i].start();
        }
// Wait for threads to finish
        for (Thread t : thread) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int putTimeouts = 0;
        for (int c = 0; c < CLIENTS; c++) {
            if (!clientCorrectLogic[c]) {
                System.err.println("Test Failed.");
                return TestStatus.TEST_FAILED;
            }
            putTimeouts += clientTimeout[c];
        }
        System.out.println("[DONE in " + (System.currentTimeMillis() - ttime) / 1000.0 + "s]");
        ttime = System.currentTimeMillis();
        if ((putTimeouts) > fmaxPuts * 0.05) {
            System.out.println("Error: too many failed keys during puts! " + putTimeouts + " timeouts.");
            return TestStatus.TEST_FAILED;
        }

// Issue all gets.
        System.out.println("Confirming... ");
        System.out.flush();
        final Boolean[] clientCorrectLogicGet = new Boolean[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            clientTimeout[i] = 0;
            clientCorrectLogicGet[i] = true;
        }
// Prepare threads
        Thread[] thread1 = new Thread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int tid = i;
            thread1[i] = new Thread() {
                public void run() {
                    final long gStartTime = System.currentTimeMillis();
// Divide keys into thread-space.
                    for (int key = fmaxPuts / CLIENTS * tid; key < fmaxPuts / CLIENTS * (tid + 1); key++) {
                        if (valueArray[key] == null) {
                            continue;
                        }

                        KVSResponse getResponse = KVSClientAPI.get(serverNodes, KVSClientAPI.intToByte(key), tid);

                        if (getResponse.status == KVSResponseStatus.TIMEOUT) {
                            clientTimeout[tid]++;
                            continue;
                        } else if (getResponse.status != KVSResponseStatus.SUCCESS) {
                            System.err.println(getResponse.errorCode);
                            System.err.println("Received error code.");
                            clientCorrectLogicGet[tid] = false;
                            return;
                        }

                        if (!Arrays.equals(getResponse.value, valueArray[key])) {
                            System.err.println("Logic error found. GET Value != PUT Value.");
                            System.err.println("Put:" + valueArray[key].toString() + ", Get:" + getResponse.value.toString());
                            clientCorrectLogicGet[tid] = false;

// client logging
                            if (KVSClientLog.enableLogging && KVSClientLog.logErrorCapacityTest) {
                                KVSClientLog clientLog = KVSClientLog.kvsClientLogs[tid];
                                KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
                                        Tests.toHumanReadableString(DatatypeConverter.printHexBinary((getResponse.requestUID == null ? "null".getBytes() : getResponse.requestUID))),
                                        KVSRequestType.GET,
                                        Tests.toHumanReadableString(DatatypeConverter.printHexBinary((getResponse.key == null ? "null".getBytes() : getResponse.key))),
                                        getResponse.errorCode,
                                        KVSResponseStatus.WRONG_VALUE);
                                clientLog.messageExchangesError.add(me);
                            }

                            return;
                        }

                        if (maxTimeMS > 0 && System.currentTimeMillis() - gStartTime > maxTimeMS) {
                            System.err.println("Test is taking too long! Exceeded specified limit: " + maxTimeMS / (double) 1000 + "s");
                            System.err.println("Thread aborting after " + (key - (fmaxPuts / CLIENTS * tid)) + " gets (~" +
                                    ((key - (fmaxPuts / CLIENTS * tid)) * valueSize / ((double) 1024 * 1024)) + " MiB).");
                            clientCorrectLogicGet[tid] = false;
                            return;
                        }
                    }
                }
            };
        }
// Start threads
        for (int i = 0; i < CLIENTS; i++) {
            thread1[i].start();
        }
// Wait for threads to finish
        for (Thread t : thread1) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int totalTimeout = 0;
        for (int c = 0; c < CLIENTS; c++) {
            if (!clientCorrectLogicGet[c]) {
                System.err.println("Test Failed.");
                return TestStatus.TEST_FAILED;
            }
            totalTimeout += clientTimeout[c];
        }
        System.out.println("[DONE in " + (System.currentTimeMillis() - ttime) / 1000.0 + "s]");
        if ((totalTimeout + putTimeouts) > fmaxPuts * 0.05) {
            System.out.println("Error: too many failed keys before crash attempted! " + totalTimeout + " timeout from gets, " + putTimeouts + " timeout from puts.");
            return TestStatus.TEST_FAILED;
        }

        return TestStatus.TEST_PASSED;
    }

    public static TestStatus testCrashVerify(final ArrayList<ServerNode> aliveList, final ArrayList<ServerNode> crashList,
                                             final int valueSize, int uMaxPuts, final long maxTimeMS, final double LOWERBOUND_ALLOWED, final double UPPERBOUND_ALLOWED,
                                             StringBuilder resultString, final int CLIENTS) {
        System.out.println("\n[ TEST Crash; Key Recovery ]");

        // Before node crash is triggered
        if (testIfAllAlive(crashList) != TestStatus.TEST_PASSED) {
            System.out.println("Error: failed servers!");
            return TestStatus.TEST_FAILED;
        }
        if (testIfAllAlive(aliveList) != TestStatus.TEST_PASSED) {
            if (TestsConfig.isStrictIsAlive()) {
                System.out.println("Error: failed servers!");
                return TestStatus.TEST_FAILED;
            }
            System.out.println("Warning! failed servers!");
        }

//final int CLIENTS = CLIENTS_COUNT;
        uMaxPuts = uMaxPuts / CLIENTS;
        final int fmaxPuts = uMaxPuts * CLIENTS;
        final byte[][] valueArray = new byte[fmaxPuts][valueSize];

// Issue all gets.
        long ttime = System.currentTimeMillis();
        int failBefore = 0;
        int timeoutBefore = 0;
        System.out.println("Determining if storage was successful... ");
        System.out.flush();
        final Boolean[] clientCorrectLogicGet = new Boolean[CLIENTS];
        final int[] clientFailed = new int[CLIENTS];
        final int[] clientTimeout = new int[CLIENTS];
        final int[] clientInvalid = new int[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            clientFailed[i] = 0;
            clientTimeout[i] = 0;
            clientInvalid[i] = 0;
            clientCorrectLogicGet[i] = true;
        }
// Prepare threads
        Thread[] thread1 = new Thread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int tid = i;
            thread1[i] = new Thread() {
                public void run() {
                    final long gStartTime = System.currentTimeMillis();
// Divide keys into thread-space.
                    for (int key = fmaxPuts / CLIENTS * tid; key < fmaxPuts / CLIENTS * (tid + 1); key++) {
                        KVSResponse getResponse = KVSClientAPI.get(aliveList, KVSClientAPI.intToByte(key), tid);

                        if (getResponse.status == KVSResponseStatus.SUCCESS) {
                            valueArray[key] = getResponse.value;
                        } else if (getResponse.status == KVSResponseStatus.TIMEOUT) {
                            valueArray[key] = null;
                            clientTimeout[tid]++;
                        } else {
                            valueArray[key] = null;
                            clientFailed[tid]++;
                        }

                        if (maxTimeMS > 0 && System.currentTimeMillis() - gStartTime > maxTimeMS) {
                            System.err.println("Test is taking too long! Exceeded specified limit: " + maxTimeMS / (double) 1000 + "s");
                            System.err.println("Thread aborting after " + (key - (fmaxPuts / CLIENTS * tid)) + " gets (~" +
                                    ((key - (fmaxPuts / CLIENTS * tid)) * valueSize / ((double) 1024 * 1024)) + " MiB).");
                            clientCorrectLogicGet[tid] = false;
                            return;
                        }
                    }
                }
            };
        }
// Start threads
        for (int i = 0; i < CLIENTS; i++) {
            thread1[i].start();
        }
// Wait for threads to finish
        for (Thread t : thread1) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int c = 0; c < CLIENTS; c++) {
            if (!clientCorrectLogicGet[c]) {
                System.err.println("Test Failed.");
                return TestStatus.TEST_FAILED;
            }
            failBefore += clientFailed[c];
            timeoutBefore += clientTimeout[c];
        }
        System.out.println("[DONE in " + (System.currentTimeMillis() - ttime) / 1000.0 + "s]");

        if ((failBefore + timeoutBefore) > fmaxPuts * 0.1) {
            System.out.println("Warning: too many failed keys before crash attempted: " + timeoutBefore + " timeout, " + failBefore + " otherwise failed.");
        }

        // Shutdown kill list.
        testShutdownList(crashList);

        // Ensure shutdown obeyed.
        if (testIfAllDead(crashList) != TestStatus.TEST_PASSED) {
            System.out.println("Error: Servers are still up after being issued shutdown command!");
            return TestStatus.TEST_FAILED;
        }
        if (testIfAllAlive(aliveList) != TestStatus.TEST_PASSED) {
            if (TestsConfig.isStrictIsAlive()) {
                System.out.println("Error: failed servers after crash issued to others!");
                return TestStatus.TEST_FAILED;
            }
            System.out.println("Warning! failed servers after crash issued to others!");

        }

        // After node crash has been triggered
        System.out.println("Attempting retrieval of keys... ");
        System.out.flush();
        ttime = System.currentTimeMillis();
        final Boolean[] clientCorrectLogicReGet = new Boolean[CLIENTS];
        int failed = 0;
        int timedOut = 0;
        int invalidValue = 0;
        for (int i = 0; i < CLIENTS; i++) {
            clientFailed[i] = 0;
            clientTimeout[i] = 0;
            clientInvalid[i] = 0;
            clientCorrectLogicReGet[i] = true;
        }
        // Prepare threads
        Thread[] threadReGet = new Thread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int tid = i;
            threadReGet[i] = new Thread() {
                public void run() {
                    final long rgStartTime = System.currentTimeMillis();
        // Divide keys into thread-space.
                    for (int key = fmaxPuts / CLIENTS * tid; key < fmaxPuts / CLIENTS * (tid + 1); key++) {
                        KVSResponse getResponse = KVSClientAPI.get(aliveList, KVSClientAPI.intToByte(key), tid);

                        if (getResponse.status == KVSResponseStatus.ERROR_KVS) {
                            clientFailed[tid]++;
                        } else if (getResponse.status == KVSResponseStatus.SUCCESS) {
                            if (!Arrays.equals(getResponse.value, valueArray[key])) {
                                clientInvalid[tid]++;

        // client logging
                                if (KVSClientLog.enableLogging && KVSClientLog.logErrorCapacityTest) {
                                    KVSClientLog clientLog = KVSClientLog.kvsClientLogs[tid];
                                    KVSClientLog.MessageExchange me = clientLog.new MessageExchange(System.currentTimeMillis(),
                                            Tests.toHumanReadableString(DatatypeConverter.printHexBinary((getResponse.requestUID == null ? "null".getBytes() : getResponse.requestUID))),
                                            KVSRequestType.GET,
                                            Tests.toHumanReadableString(DatatypeConverter.printHexBinary((getResponse.key == null ? "null".getBytes() : getResponse.key))),
                                            getResponse.errorCode,
                                            KVSResponseStatus.WRONG_VALUE);
                                    clientLog.messageExchangesError.add(me);
                                }

                            }
                        } else if (getResponse.status == KVSResponseStatus.TIMEOUT) {
                            clientTimeout[tid]++;
                        }

                        if (maxTimeMS > 0 && System.currentTimeMillis() - rgStartTime > maxTimeMS) {
                            System.err.println("Test is taking too long! Exceeded specified limit: " + maxTimeMS / (double) 1000 + "s");
                            System.err.println("Thread aborting after " + (key - (fmaxPuts / CLIENTS * tid)) + " gets (~" +
                                    ((key - (fmaxPuts / CLIENTS * tid)) * valueSize / ((double) 1024 * 1024)) + " MiB).");
                            clientCorrectLogicReGet[tid] = false;
                            return;
                        }
                    }
                }
            };
        }
        // Start threads
        for (int i = 0; i < CLIENTS; i++) {
            threadReGet[i].start();
        }
        // Wait for threads to finish
        for (Thread t : threadReGet) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int c = 0; c < CLIENTS; c++) {
            if (!clientCorrectLogicReGet[c]) {
                System.err.println("Test Failed.");
                return TestStatus.TEST_FAILED;
            }
            failed += clientFailed[c];
            timedOut += clientTimeout[c];
            invalidValue += clientInvalid[c];
        }
        System.out.println("[DONE in " + (System.currentTimeMillis() - ttime) / 1000.0 + "s]");
        ttime = System.currentTimeMillis();

        // Ensure shutdown obeyed.
        if (testIfAllDead(crashList) != TestStatus.TEST_PASSED) {
            System.out.println("Error: Servers are still up after being issued shutdown command!");
            return TestStatus.TEST_FAILED;
        }
        if (testIfAllAlive(aliveList) != TestStatus.TEST_PASSED) {
            if (TestsConfig.isStrictIsAlive()) {
                System.out.println("Error: failed servers!");
                return TestStatus.TEST_FAILED;
            }
            System.out.println("Warning! failed servers!");
        }

        double percentExpected = (crashList.size()*100.0) / (aliveList.size()+crashList.size()*1.0);
        double percentFailed = ((failed + timedOut + invalidValue) * 100.0) / (fmaxPuts - failBefore - timeoutBefore);
        System.out.println("Of " + (fmaxPuts - failBefore - timeoutBefore) + " usable puts:");
        System.out.println("Failed: " + failed + " Timed out: " + timedOut + " Invalid value: " + invalidValue);
        System.out.println("Expecting " + percentExpected + "% keys to fail. Actual failed: " + percentFailed + "%");
        System.out.println("Keys failed after server crash : " + percentFailed + "%");
        resultString.append(percentFailed);


        if (percentFailed > UPPERBOUND_ALLOWED * 100 || percentFailed < LOWERBOUND_ALLOWED * 100) {
            System.out.println("Percentage of failed keys should be between " + LOWERBOUND_ALLOWED*100 + "% and " + UPPERBOUND_ALLOWED*100 + "%.");
            System.out.println("Failed: " + failed + " Timed out: " + timedOut + " Invalid value: " + invalidValue);
            return TestStatus.TEST_FAILED;
        }

        return TestStatus.TEST_PASSED;
    }

}
