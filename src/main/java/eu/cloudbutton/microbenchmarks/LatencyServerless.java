package eu.cloudbutton.microbenchmarks;

import com.amazonaws.regions.Regions;
import crucial.execution.ServerlessExecutorService;
import crucial.execution.aws.AWSLambdaExecutorService;
import crucial.execution.aws.Config;
import eu.cloudbutton.mandelbrot.Utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class LatencyServerless {

    static {
        Config.functionName="CloudTread-utslambda";
        Config.region= Regions.EU_WEST_1;
    }

    private static ServerlessExecutorService awsExecutorService;
    private static int N = 1000;

    private void submitAndGet(){
        Future<Result> future = awsExecutorService.submit(new DummyCallable("Hello"));
        try {
            Result result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void warmUp() {
        long t;
        for (int i= 0;i<10;i++) {
            t = -System.nanoTime();
            submitAndGet();
            t += System.nanoTime();
            System.out.println(t);
        }
    }

    public void run() {
        //long t;
        for (int i= 0;i<N;i++){
            //t = -System.nanoTime();
            submitAndGet();
            //t += System.nanoTime();
            //System.out.println(t);
        }
    }

    public static void main(String[] args) {
        try{
            System.out.println("Latency overhead [Serverless]");

            awsExecutorService = new AWSLambdaExecutorService();
            awsExecutorService.setLogs(false);
            LatencyServerless ls = new LatencyServerless();

            System.out.println("Warmup...");
            ls.warmUp();

            System.out.println("Starting...");
            long time = -System.nanoTime();

            ls.run();

            time += System.nanoTime();
            System.out.println("Finished.");

            System.out.println("N: " + N + " Avg. time: " + Utils.sub("" + time / (N * 1e6), 0, 6) + "ms");

        } finally {
            awsExecutorService.shutdown();
        }
    }
}
