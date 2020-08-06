package crucial.execution.aws;


import com.amazonaws.services.lambda.model.InvokeResult;

import crucial.execution.ServerlessExecutorService;

import java.util.Base64;

public class AWSLambdaExecutorService extends ServerlessExecutorService {
    private AWSLambdaInvoker invoker = new AWSLambdaInvoker(Config.region, Config.functionName);

    private AWSCostReport costReport = new AWSCostReport();
    
    @Override
    protected byte[] invokeExternal(byte[] threadCall) {
        if (logs) {
            System.out.println(this.printPrefix() + "Calling AWS Lambda.");
        }
        InvokeResult result = invoker.invoke(threadCall);
        if (logs) {
            System.out.println(this.printPrefix() + "AWS call completed.");
        }
        if (logs || costReporting) {
            String tailLogs = new String(Base64.getDecoder().decode(result.getLogResult()));
            if (costReporting) {
                String s = tailLogs.lines().filter(p -> p.startsWith("REPORT ")).findFirst().get().replaceFirst(".*Billed Duration: (\\d*) ms.*", "$1");
                int duration = Integer.parseInt(s);
                costReport.addExecutionDuration(duration);
                if (costReport.getMemoryConfigured() == 0) {
                    String sm = tailLogs.lines().filter(p -> p.startsWith("REPORT ")).findFirst().get().replaceFirst(".*Memory Size: (\\d*) MB.*", "$1");
                    int memory = Integer.parseInt(sm);
                    costReport.setMemoryConfigured(memory);
                }
            }
            if (logs) {
                System.out.println(this.printPrefix() + "Showing Lambda Tail Logs.\n");
                assert result != null;
                System.out.println(tailLogs);
            }
        }
        return Base64.getMimeDecoder().decode(result.getPayload().array());
    }
    
    public String printCostReport() {
        return costReport.printCostReport();
    }
    
    public void resetCostReport() {
        costReport.reset();
    }

    public void closeInvoker() {
        invoker.stop();
    }
}
