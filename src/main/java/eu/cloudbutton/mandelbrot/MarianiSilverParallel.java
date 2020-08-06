package eu.cloudbutton.mandelbrot;

import eu.cloudbutton.mandelbrot.serverless.PlotData;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MarianiSilverParallel {

    static int RECTANGLE_SIZE_THRESHOLD = 8 * 8;
    static int MAX_DEPTH = 4; //5;
    static int SPLIT_FACTOR = 4;
    static int MAX_DWELL = 5_000_000; // 1_000; //512;
    static int INIT_SUBDIV = 16*16; //8*8;
    
    private static boolean SAVE_IMAGE = true;
    

    private static ExecutorService localExecutorService;

    private MandelbrotImage image;

    AtomicLong counter;
    AtomicLong activeThreads;
    Deque<Rectangle> queue = new ConcurrentLinkedDeque<>();

    public MarianiSilverParallel(int width, int height) {
        image = new MandelbrotImage();
        image.init(width, height);
        
        this.counter = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0); 
    }

    public void run(Rectangle rectangle) {
        
        List<Rectangle> initSubRectangles = rectangle.split(INIT_SUBDIV);
        submitRectangles(initSubRectangles);
        

        while (activeThreads.get() > 0 || !queue.isEmpty()) {

            Rectangle r = queue.poll();
            if (r != null) {
                activeThreads.addAndGet(-1);
                
                if (r.isReadyToSplit()) {
                    List<Rectangle> subRectangles = r.split(SPLIT_FACTOR);
                    submitRectangles(subRectangles);
                }
            }
        }

    }

    private void submitRectangle(Rectangle rectangle) {
        activeThreads.addAndGet(1);
        localExecutorService.submit(new MarianiSilverRunnable(rectangle, image, queue));
    }

    private void submitRectangles(List<Rectangle> rectangles) {
        for (Rectangle r : rectangles) {
            submitRectangle(r);
        }
    }

    private void saveImage() {
        image.saveToFile("mandelbrot.png");
    }

    public static void main(String[] args) {
        CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        try {
            System.out.println("MarianiSilver - Workers set to " + opts.workers);
            localExecutorService = Executors.newFixedThreadPool(opts.workers);

            MarianiSilverParallel ms = new MarianiSilverParallel(opts.width, opts.height);
            Rectangle rectangle = new Rectangle(opts.width, opts.height);

            System.out.println("Starting...");
            long time = -System.nanoTime();

            ms.run(rectangle);

            time += System.nanoTime();
            System.out.println("Finished.");

            System.out.println("Max Dwell: " + MAX_DWELL + ", Size: " + opts.width + "x" + opts.height + ", Threshold: "
                    + RECTANGLE_SIZE_THRESHOLD + ", Split factor: " + SPLIT_FACTOR + ", Max. depth: " + MAX_DEPTH);
            System.out.println("Time: " + Utils.sub("" + time / 1e9, 0, 6) + "s");
            System.out.println("Performance: " + Utils.sub("" + (opts.width*opts.height / (time / 1e3)), 0, 6) + " Mpix/s");

            if (SAVE_IMAGE) {
                ms.saveImage();
            }

        } finally {
            localExecutorService.shutdown();
            System.out.println("finish");
        }

    }

}
