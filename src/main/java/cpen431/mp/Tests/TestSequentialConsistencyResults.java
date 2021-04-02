package cpen431.mp.Tests;

import cpen431.mp.KeyValue.KVSResultField;
import java.util.ArrayList;

public class TestSequentialConsistencyResults {
    public long numSuccess = 0;
    public long numTimeout = 0;
    public long numError = 0;
    public long numInvalidValue = 0;
    public long numInvalidVersion = 0;
    public long numInvalidNotRemoved = 0;

    public long totalRequests() {
        return numSuccess + numTimeout + numError + numInvalidValue + numInvalidVersion + numInvalidNotRemoved;
    }

    public void printSummary() {
        long total = totalRequests();

        System.out.println("Total Responses: " + total);
        System.out.println("Success Responses: " + numSuccess + ", % Success = " + (numSuccess * 100.0 / total) + "%");
        System.out.println("Timeout Responses: " + numTimeout + ", % Timeout = " + (numTimeout * 100.0 / total) + "%");
        System.out.println("Error Responses: " + numError + ", % Error = " + (numError * 100.0 / total) + "%");
        System.out.println("Invalid Value Responses: " + numInvalidValue + ", % Invalid Value = " + (numInvalidValue * 100.0 / total) + "%");
        System.out.println("Invalid Version Responses: " + numInvalidVersion + ", % Invalid Version = " + (numInvalidVersion * 100.0 / total) + "%");
        System.out.println("Keys Not Removed: " + numInvalidNotRemoved);
    }

    public void printSummary(ArrayList<KVSResultField> resultMap) {
        long total = totalRequests();
        double perSuccess = numSuccess * 100.0 / total;
        double perTimeout = numTimeout * 100.0 / total;
        double perError = numError * 100.0 / total;
        double perInvalidValue = numInvalidValue * 100.0 / total;
        double perInvalidVersion = numInvalidVersion * 100.0 / total;

        System.out.println("Total Responses: " + total);
        System.out.println("Success Responses: " + numSuccess + ", % Success = " + perSuccess + "%");
        System.out.println("Timeout Responses: " + numTimeout + ", % Timeout = " + perTimeout + "%");
        System.out.println("Error Responses: " + numError + ", % Error = " + perError + "%");
        System.out.println("Invalid Value Responses: " + numInvalidValue + ", % Invalid Value = " + perInvalidValue + "%");
        System.out.println("Invalid Version Responses: " + numInvalidVersion + ", % Invalid Version = " + perInvalidVersion + "%");
        System.out.println("Keys Not Removed: " + numInvalidNotRemoved);

        resultMap.add(new KVSResultField("Rolling SC Total Responses", Long.toString(total)));
        resultMap.add(new KVSResultField("Rolling SC Success Responses", Long.toString(numSuccess)));
        resultMap.add(new KVSResultField("Rolling SC Timeout Responses", Long.toString(numTimeout)));
        resultMap.add(new KVSResultField("Rolling SC Error Responses", Long.toString(numError)));
        resultMap.add(new KVSResultField("Rolling SC Invalid Value Responses", Long.toString(numInvalidValue)));
        resultMap.add(new KVSResultField("Rolling SC Invalid Version Responses", Long.toString(numInvalidVersion)));
        resultMap.add(new KVSResultField("Rolling SC Keys Not Removed", Long.toString(numInvalidNotRemoved)));

        resultMap.add(new KVSResultField("Rolling SC % Success", Double.toString(perSuccess)));
        resultMap.add(new KVSResultField("Rolling SC % Timeout", Double.toString(perTimeout)));
        resultMap.add(new KVSResultField("Rolling SC % Error", Double.toString(perError)));
        resultMap.add(new KVSResultField("Rolling SC % Invalid Value", Double.toString(perInvalidValue)));
        resultMap.add(new KVSResultField("Rolling SC % Invalid Version", Double.toString(perInvalidVersion)));
    }
}