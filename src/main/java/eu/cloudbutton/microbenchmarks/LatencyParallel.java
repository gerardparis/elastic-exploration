package eu.cloudbutton.microbenchmarks;

import eu.cloudbutton.mandelbrot.Utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LatencyParallel {

    private static ExecutorService localExecutorService;
    private static int N = 1_000_000;

    private void submitAndGet(){
        Future<Result> future = localExecutorService.submit(new DummyCallable("Hello"));
        try {
            Result result = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void warmUp() {
        long t;
        for (int i= 0; i<10; i++) {
            t = -System.nanoTime();
            submitAndGet();
            t += System.nanoTime();
            System.out.println(t);
        }
    }

    public void run() {
        for (int i= 0; i<N; i++){
            submitAndGet();
        }
    }

    public static void main(String[] args) {
        try{
            System.out.println("Latency overhead [Parallel]");

            localExecutorService = Executors.newFixedThreadPool(1);
            LatencyParallel lp = new LatencyParallel();

            System.out.println("Warmup...");
            lp.warmUp();

            System.out.println("Starting...");
            long time = -System.nanoTime();

            lp.run();

            time += System.nanoTime();
            System.out.println("Finished.");

            System.out.println(time);
            System.out.println("N: " + N + " Avg. time: " + Utils.sub("" + time / (N * 1e3), 0, 6) + "us");

        } finally {
            localExecutorService.shutdown();
        }
    }
}
