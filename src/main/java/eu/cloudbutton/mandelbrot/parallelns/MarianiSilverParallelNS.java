package eu.cloudbutton.mandelbrot.parallelns;

import com.amazonaws.regions.Regions;
import crucial.execution.ServerlessExecutorService;
import crucial.execution.aws.AWSLambdaExecutorService;
import crucial.execution.aws.Config;
import eu.cloudbutton.mandelbrot.CmdLineOptions;
import eu.cloudbutton.mandelbrot.MandelbrotImage;
import eu.cloudbutton.mandelbrot.Rectangle;
import eu.cloudbutton.mandelbrot.Utils;

import java.awt.*;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This version does not use shared data structures between threads.
 */
public class MarianiSilverParallelNS {

    static int RECTANGLE_SIZE_THRESHOLD = 8 * 8;
    static int MAX_DEPTH = 5;
    static int SPLIT_FACTOR = 4;
    static int MAX_DWELL = 1_000_000; // 1_000; //512;
    static int INIT_SUBDIV = 8 * 8;

    private static boolean SAVE_IMAGE = true;

    private static final int BLACK = Color.BLACK.getRGB();

    private static int[] colors;

    static {
        Config.functionName = "CloudTread-utslambda";
        Config.region = Regions.EU_WEST_1;

        colors = new int[MarianiSilverParallelNS.MAX_DWELL];
        for (int i = 0; i < MarianiSilverParallelNS.MAX_DWELL; i++) {
            colors[i] = Color.HSBtoRGB(i / 256f, 1, i / (i + 8f)); // brownish
            // colors[i] = Color.HSBtoRGB(0.95f + 10*((float)i)/MAX_DWELL, 0.6f, 1f); // pinkish
        }
    }

    private static ExecutorService localExecutorService;

    private MandelbrotImage image;

    AtomicLong counter;
    AtomicLong activeThreads;
    Deque<Result> queue = new ConcurrentLinkedDeque<>();

    public MarianiSilverParallelNS(int width, int height) {
        image = new MandelbrotImage();
        image.init(width, height);

        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0);
    }

    public void run(Rectangle rectangle) {

        // submitRectangle(rectangle);

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
            Result result = new Result(rectangle);
            if (borderHasCommonDwell(rectangle)) {
                // System.out.println("Computing border...");
                result.setNextAction(Result.Action.FILL);
                result.setDwellToFill(rectangle.getBorderDwell());
            } else if (rectangle.getDepth() >= MarianiSilverParallelNS.MAX_DEPTH
                    || rectangle.size() <= MarianiSilverParallelNS.RECTANGLE_SIZE_THRESHOLD) {
                // per-pixel evaluation of the rectangle
                int[][] dwellArray = evaluate(rectangle);
                result.setNextAction(Result.Action.SET_DWELL_ARRAY);
                result.setDwellArray(dwellArray);

            } else {
                result.setNextAction(Result.Action.SPLIT);
            }

            queue.offer(result);
        }

        private int evaluatePoint(int col, int row, int width, int height) {
            double x0 = (col - width / 2) * 4.0 / width;
            double y0 = (row - height / 2) * 4.0 / width;
            double x = 0, y = 0;
            int iteration = 0;
            while (x * x + y * y < 4 && iteration < MarianiSilverParallelNS.MAX_DWELL) {
                double xTemp = x * x - y * y + x0;
                y = 2 * x * y + y0;
                x = xTemp;
                iteration++;
            }
            return iteration;
        }

        private int[][] evaluate(Rectangle rectangle) {
            int[][] dwellArray = new int[rectangle.getY1()-rectangle.getY0()+1][rectangle.getX1()-rectangle.getX0()+1];

            for (int row = 0; row < dwellArray.length; row++) {
                for (int col = 0; col < dwellArray[0].length; col++) {
                    dwellArray[row][col] = evaluatePoint(rectangle.getX0() + col, rectangle.getY0() + row, rectangle.getMainWidth(), rectangle.getMainHeight());

                }
            }
            return dwellArray;
        }

        private boolean borderHasCommonDwell(Rectangle rectangle) {
            int x0 = rectangle.getX0();
            int y0 = rectangle.getY0();
            int x1 = rectangle.getX1();
            int y1 = rectangle.getY1();
            int x = x0;
            int y = y0;

            int currentDwell = -1;

            while (currentDwell == -1 || (x != x0 || y != y0)) {
                int dwell = evaluatePoint(x, y, rectangle.getMainWidth(), rectangle.getMainHeight());
                if (currentDwell == -1) {
                    // Set the dwell of the first point
                    currentDwell = dwell;
                } else if (currentDwell != dwell) {
                    return false;
                }

                // next border point?
                x = nextX(x, y, x0, y0, x1, y1);
                y = nextY(x, y, x0, y0, x1, y1);
            }

            rectangle.setBorderDwell(currentDwell);

            return true;
        }

        int nextX(int x, int y, int x0, int y0, int x1, int y1) {
            int delta = 0;
            if (x < x1 && y == y0) {
                delta = +1;
            } else if (x > x0 && y == y1) {
                delta = -1;
            }
            return x + delta;
        }

        int nextY(int x, int y, int x0, int y0, int x1, int y1) {
            int delta = 0;
            if (y < y1 && x == x1) {
                delta = +1;
            } else if (y > y0 && x == x0) {
                delta = -1;
            }
            return y + delta;
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
        if (dwell < MarianiSilverParallelNS.MAX_DWELL)
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
            System.out.println("MarianiSilver [ParallelNS] - Workers set to " + opts.workers);
            localExecutorService = Executors.newFixedThreadPool(opts.workers);

            MarianiSilverParallelNS ms = new MarianiSilverParallelNS(opts.width, opts.height);
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

            if (SAVE_IMAGE) {
                ms.saveImage();
            }
        } finally {
            localExecutorService.shutdown();
            System.out.println("finish");
        }

    }

}
