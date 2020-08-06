package eu.cloudbutton.utslambda;

import crucial.execution.ServerlessExecutorService;
import crucial.execution.aws.AWSLambdaExecutorService;
import crucial.execution.aws.Config;
import eu.cloudbutton.utslambda.serverless.taskmanager.PlotData;
import eu.cloudbutton.utslambda.serverless.taskmanager.TMBagWorkerCallable;
import eu.cloudbutton.utslambda.serverless.taskmanager.TMResult;
import eu.cloudbutton.utslambda.serverless.taskmanager.TaskStats;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ServerlessUTSWithPlot {
    static {
        Config.functionName = "CloudTread-utslambda";
    }

    public static long run(final int parallelism, final int numberOfIterationsPerWave, final int depth) {
        refTs = System.currentTimeMillis();
        ServerlessUTSWithPlot waves = new ServerlessUTSWithPlot(parallelism, numberOfIterationsPerWave);
        waves.run(depth);
        return waves.count;
    }

    private ServerlessUTSWithPlot(final int parallelism, final int numberOfIterationsPerWave) {

        this.parallelism = parallelism;
        this.numberOfIterationsPerWave = numberOfIterationsPerWave;
        this.count = 0;
    }

    private static ServerlessExecutorService executorService;

    private final int parallelism;
    private final int numberOfIterationsPerWave;
    private long count;

    private void run(List<Bag> bags) {
        while (!bags.isEmpty()) {
            bags = doSimpleRound(bags);
        }
    }

    private void run(final int depth) {
        final Bag initBag = new Bag(64);
        final MessageDigest md = Utils.encoder();
        initBag.seed(md, 19, depth);

        final List<Bag> bags = new ArrayList<Bag>();
        bags.add(initBag);
        run(bags);
    }

    //static List<Map.Entry<Long, Long>> finishTimes = Collections.synchronizedList(new ArrayList<>());
    static List<TaskStats> taskStatsList = Collections.synchronizedList(new ArrayList<>());
    static long refTs;

    public List<Bag> doSimpleRound(List<Bag> bags) {
        Utils.resizeBags(bags, parallelism);

        // Place bags in groups of <i>groupSize</i> bags
        // int groupSize = 1;
        // List<List<Bag>> groupedBags = Utils.groupBags(bags, groupSize);

        ArrayList<Bag> results = new ArrayList<>();
        parallelize(bags, results, bags.size());

        List<Bag> compactedBags = new ArrayList<Bag>(results.size());

        this.count += Utils.coalesceAndCount(results, compactedBags);
        System.out.println("End of round count: " + this.count);
        System.out.println("Ecompacted bags: " + compactedBags.size());
        return compactedBags;

    }

    public void parallelize(List<Bag> bags, List<Bag> results, int size) {

        List<Callable<TMResult>> myTasks = Collections.synchronizedList(new ArrayList<>());

        // long initTime = System.currentTimeMillis();
        for (int w = 0; w < size; w++) {
            myTasks.add(new TMBagWorkerCallable(bags.get(w), numberOfIterationsPerWave));
        }

        System.out.println("Launching " + myTasks.size() + " remote functions...");
        List<Future<TMResult>> futures;
        try {
            futures = executorService.invokeAll(myTasks);
            for (Future<TMResult> future : futures) {
                TMResult tmResult = future.get();
                // System.out.println("Returned a bag of size " + resultBag.get(0).size + " and
                // count " + resultBag.get(0).count);
                results.add(tmResult.getBag());
                //finishTimes.add(Map.entry(tmResult.getEndTs() - refTs, tmResult.getEndTs() - tmResult.getInitTs()));
                taskStatsList.add(new TaskStats(tmResult.getBag().bagId,
                        tmResult.getBag().parentBagId,
                        tmResult.getInitTs() - refTs,
                        tmResult.getEndTs() - tmResult.getInitTs()));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        // long endTime = System.currentTimeMillis();

    }

    public static void main(String[] args) {

        final CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            executorService = new AWSLambdaExecutorService();
            executorService.setLogs(false);

            final int parallelism = 2000;
            final int numberOfIterationsPerWave = 10_000_000; //5_000_000;

            /*
             * // Warmup phase if (opts.warmupDepth > 0) { System.out.println("Warmup...");
             * run(parallelism, numberOfIterationsPerWave, opts.warmupDepth);
             * executorService.resetCostReport(); }
             */

            System.out.println("Starting...");
            long time = -System.nanoTime();
            final long count = run(parallelism, numberOfIterationsPerWave, opts.depth);
            time += System.nanoTime();
            System.out.println("Finished.");

            System.out
                    .println("Depth: " + opts.depth + ", Performance: " + count + "/" + Utils.sub("" + time / 1e9, 0, 6)
                            + " = " + Utils.sub("" + (count / (time / 1e3)), 0, 6) + "M nodes/s");
            System.out.println(executorService.printCostReport());

            PlotData.plotConcurrency(taskStatsList);

        } finally {
            executorService.shutdown();
            System.out.println("finish");
        }
    }
}