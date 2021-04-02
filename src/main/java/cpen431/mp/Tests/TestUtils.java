package cpen431.mp.Tests;

import cpen431.mp.KeyValue.KVSResultField;
import cpen431.mp.Utilities.Checksum;
import cpen431.mp.Utilities.ServerNode;

import java.util.ArrayList;
import java.util.Collections;

public class TestUtils {

    public static ArrayList<ServerNode> getServerNodes(String fileName) {
        ArrayList<ServerNode> bigListServerNodes = null;
        try {
            bigListServerNodes = ServerNode.buildServerNodeList(fileName);
            System.out.println("Done building node list.");
        }
        catch (Exception e) {
            System.err.println("Specified server list file not found!");
            e.printStackTrace();
        }
        return bigListServerNodes;
    }

    public static String printResultMapSummary(ArrayList<KVSResultField> resultMap) {
        String results = "";
        String code;

        System.out.println("Results Summary:");

        Collections.sort(resultMap);

        for (KVSResultField entry : resultMap) {
            System.out.println(entry.key + " : " + entry.value);
            results += entry.value;
        }

        code = Checksum.getMD5(results);
        System.out.println("Result Verification Code : " + code);

        return code;
    }

}

