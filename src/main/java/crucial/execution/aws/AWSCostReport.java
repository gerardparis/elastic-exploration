package crucial.execution.aws;

import java.util.List;

import crucial.execution.CostReport;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;

public class AWSCostReport implements CostReport{

    double AWS_LAMBDA_REQUEST_COST = 0.0000002; // $ per request
    double AWS_LAMBDA_EXECUTION_COST = 0.0000166667; // $ per GB-second used

    private List<Integer> executionDuration = Collections.synchronizedList(new ArrayList<>());
    private int memoryConfigured = 0;

    public void setMemoryConfigured(int memory) {
        this.memoryConfigured = memory;
    }

    public int getMemoryConfigured() {
        return this.memoryConfigured;
    }

    public void addExecutionDuration(int duration) {
        executionDuration.add(duration);
    }

    public void reset() {
        executionDuration = new ArrayList<>();
        memoryConfigured = 0;
    }

    public String printCostReport() {
        if (memoryConfigured == 0 || executionDuration.isEmpty()) {
            System.out.println(memoryConfigured);
            return "Could not compute the Cost report! Insufficient data.";
        } else {
            int sum = executionDuration.stream().mapToInt(Integer::intValue).sum();

            double requestsCost = executionDuration.size() * AWS_LAMBDA_REQUEST_COST;
            double executionCost = sum / 1000.0 * AWS_LAMBDA_EXECUTION_COST * memoryConfigured / 1024.0;
            double totalCost = requestsCost + executionCost;

            NumberFormat formatter2d = new DecimalFormat("#0.00");
            NumberFormat formatter4d = new DecimalFormat("#0.0000");
            return "Total executions " + executionDuration.size() + "\tMem. Size " + memoryConfigured
                    + " MB\tTotal Billed Duration " + sum + " ms\tCost " + formatter4d.format(requestsCost) + " + "
                    + formatter4d.format(executionCost) + " = " + formatter2d.format(totalCost) + " $";
        }
    }

}
