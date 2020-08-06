package eu.cloudbutton.mandelbrot.serverless;

import com.amazonaws.regions.Regions;
import crucial.execution.ServerlessExecutorService;
import crucial.execution.ServerlessHybridExecutorService;
import crucial.execution.aws.AWSLambdaExecutorService;
import crucial.execution.aws.AWSLambdaHybridExecutorService;
import crucial.execution.aws.Config;
import eu.cloudbutton.mandelbrot.CmdLineOptions;
import eu.cloudbutton.mandelbrot.MandelbrotImage;
import eu.cloudbutton.mandelbrot.Rectangle;
import eu.cloudbutton.mandelbrot.Utils;

import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MarianiSilverServerlessHybrid {

    static int RECTANGLE_SIZE_THRESHOLD = 8 * 8;
    static int MAX_DEPTH = 4; //5;
    static int SPLIT_FACTOR = 4;
    static int MAX_DWELL = 5_000_000; // 1_000; //512;
    static int INIT_SUBDIV = 16*16; //8 * 8;

    private static boolean SAVE_IMAGE = true;

    private static final int BLACK = Color.BLACK.getRGB();

    private static int[] colors;

    static {
        Config.functionName = "CloudTread-utslambda";
        Config.region = Regions.EU_WEST_1;

        colors = new int[MarianiSilverServerlessHybrid.MAX_DWELL];
        for (int i = 0; i < MarianiSilverServerlessHybrid.MAX_DWELL; i++) {
            colors[i] = Color.HSBtoRGB(i / 256f, 1, i / (i + 8f)); // brownish
            // colors[i] = Color.HSBtoRGB(0.95f + 10*((float)i)/MAX_DWELL, 0.6f, 1f); // pinkish
        }
    }

    private static ServerlessHybridExecutorService awsExecutorService;
    private static ExecutorService localExecutorService;

    private MandelbrotImage image;

    AtomicLong counter;
    AtomicLong activeThreads;
    Deque<Result> queue = new ConcurrentLinkedDeque<>();
    static List<TaskStats> taskStatsList = Collections.synchronizedList(new ArrayList<>());
    static Long refTs;

    public MarianiSilverServerlessHybrid(int width, int height) {
        image = new MandelbrotImage();
        image.init(width, height);

        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0);
    }

    public void run(Rectangle rectangle) {

        // submitRectangle(rectangle);

        refTs = System.currentTimeMillis();
        List<Rectangle> initSubRectangles = rectangle.split(INIT_SUBDIV);
        submitRectangles(initSubRectangles);

        while (activeThreads.get() > 0 || !queue.isEmpty()) {

            Result result = queue.poll();
            if (result != null) {
                activeThreads.addAndGet(-1);
                
                Rectangle r = result.getRectangle();

                System.out.println(result.getNextAction().toString());
                
                switch (result.getNextAction()) {
                case SPLIT:

                    List<Rectangle> subRectangles = r.split(SPLIT_FACTOR);
                    submitRectangles(subRectangles);
                    break;
                    
                case FILL:
                    image.fillColor(r, getColor(result.getDwellToFill()));
                    break;
                case SET_DWELL_ARRAY:
                    int[][] colorArray = getColorArray(result.getDwellArray());
                    image.setColor(r.getY0(), r.getX0(), colorArray);
                    break;
                }

            }
        }

    }

    class LocalRunnable implements Runnable {
        Rectangle rectangle;

        public LocalRunnable(Rectangle rectangle) {
            this.rectangle = rectangle;
        }

        @Override
        public void run() {
            Future<Result> future = awsExecutorService.submit(new MarianiSilverServerlessCallable(rectangle));
            Result result;
            try {
                result = future.get();
                queue.offer(result);

                taskStatsList.add(new TaskStats(
                        result.getInitTs() - refTs,
                        result.getEndTs() - result.getInitTs(),
                        result.getExecType()));
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }

    }

    private void submitRectangle(Rectangle rectangle) {
        activeThreads.addAndGet(1);
        localExecutorService.submit(new LocalRunnable(rectangle));
    }

    private void submitRectangles(List<Rectangle> rectangles) {
        for (Rectangle r : rectangles) {
            submitRectangle(r);
        }
    }

    private static int getColor(int dwell) {
        if (dwell < MarianiSilverServerlessHybrid.MAX_DWELL)
            return colors[dwell];
        else
            return BLACK;
    }
    
    private static int[][] getColorArray(int[][] dwellArray) {
        int[][] colorArray = new int[dwellArray.length][dwellArray[0].length];
        for (int i=0;i<dwellArray.length;i++) {
            for (int j=0;j<dwellArray[i].length;j++) {
                colorArray[i][j] = getColor(dwellArray[i][j]);
            }
        }
        return colorArray;
    }

    private void saveImage() {
        image.saveToFile("mandelbrot.png");
    }

    public static void main(String[] args) {
        CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            System.out.println("MarianiSilver [ServerlessHybrid]");
            System.out.println("Local workers set to " + opts.workers);

            awsExecutorService = new AWSLambdaHybridExecutorService(opts.workers);
            awsExecutorService.setLogs(false);

            localExecutorService = Executors.newFixedThreadPool(2000);

            MarianiSilverServerlessHybrid ms = new MarianiSilverServerlessHybrid(opts.width, opts.height);
            Rectangle rectangle = new Rectangle(opts.width, opts.height);

            System.out.println("Starting...");
            long time = -System.nanoTime();

            ms.run(rectangle);

            time += System.nanoTime();
            System.out.println("Finished.");

            System.out.println("Max Dwell: " + MAX_DWELL + ", Size: " + opts.width + "x" + opts.height + ", Threshold: "
                    + RECTANGLE_SIZE_THRESHOLD + ", Split factor: " + SPLIT_FACTOR + ", Max. depth: " + MAX_DEPTH);
            System.out.println("Time: " + Utils.sub("" + time / 1e9, 0, 6) + "s");
            System.out.println(
                    "Performance: " + Utils.sub("" + (opts.width * opts.height / (time / 1e3)), 0, 6) + " Mpix/s");
            System.out.println(awsExecutorService.printCostReport());
            printActualComputeTime();

            if (SAVE_IMAGE) {
                ms.saveImage();
            }

            PlotData.plotConcurrencyHybrid(taskStatsList);
        } finally {
            awsExecutorService.shutdown();
            localExecutorService.shutdown();
            System.out.println("finish");
        }

    }

    private static void printActualComputeTime() {
        long actualTime = 0L;
        for (TaskStats stats : taskStatsList) {
            actualTime += stats.getDuration();
        }
        System.out.println("Actual compute time: " + actualTime);

    }

}
