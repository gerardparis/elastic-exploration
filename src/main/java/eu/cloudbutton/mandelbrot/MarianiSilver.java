package eu.cloudbutton.mandelbrot;

import java.awt.Color;
import java.util.List;

public class MarianiSilver {

    private static int RECTANGLE_SIZE_THRESHOLD = 8*8;//32*32; // 8*8;
    private static int MAX_DEPTH = 5;
    private static int SPLIT_FACTOR = 4;
    private static int MAX_DWELL = 512; //1_000; //512; 
    private static int INIT_SUBDIV = 8*8;
    
    private static final int BLACK = Color.BLACK.getRGB();
    
    private static boolean SAVE_IMAGE = true;

    private static int[] colors;
    
    static {
        colors = new int[MAX_DWELL];
        for (int i = 0; i < MAX_DWELL; i++) {
            //colors[i] = Color.HSBtoRGB(i / 256f, 1, i / (i + 8f)); // brownish
            colors[i] = Color.HSBtoRGB(256/360f, 1, i / (i + 8f)); // blueish
            //colors[i] = Color.HSBtoRGB(0.95f + 10*((float)i)/MAX_DWELL, 0.6f, 1f); // pinkish
        }
    }

    private MandelbrotImage image;
    private static int evaluatedPoints = 0;

    public MarianiSilver(int width, int height) {
        image = new MandelbrotImage();
        image.init(width, height);
    }
    
    private static int getColor(int dwell) {
        if (dwell < MAX_DWELL)
            return colors[dwell];
        else
            return BLACK;
    }

    public void marianiSilver(Rectangle rectangle) {
        //System.out.println("Computing rectangle" + rectangle + "...");

        if (borderHasCommonDwell(rectangle)) {
            //System.out.println("Computing border...");
            image.fillColor(rectangle, getColor(rectangle.getBorderDwell()));
        } else if (rectangle.getDepth() >= MAX_DEPTH  
                || rectangle.size() <= RECTANGLE_SIZE_THRESHOLD) {
            // per-pixel evaluation of the rectangle
            //System.out.println("Computing per-pixel...");
            evaluateAndSet(rectangle);
        } else {
            //System.out.println("Splitting...");
            List<Rectangle> subRectangles = rectangle.split(SPLIT_FACTOR);
            for (Rectangle subr : subRectangles) {
                marianiSilver(subr);
            }
        }
    }

    private int evaluatePoint(int col, int row, int width, int height) {
        double x0 = (col - width / 2) * 4.0 / width;
        double y0 = (row - height / 2) * 4.0 / width;
        double x = 0, y = 0;
        int iteration = 0;
        while (x * x + y * y < 4 && iteration < MAX_DWELL) {
            double xTemp = x * x - y * y + x0;
            y = 2 * x * y + y0;
            x = xTemp;
            iteration++;
        }
        evaluatedPoints ++;
        return iteration;
    }

    private void evaluateAndSet(Rectangle rectangle) {
        for (int row = rectangle.getY0(); row <= rectangle.getY1(); row++) {
            for (int col = rectangle.getX0(); col <= rectangle.getX1(); col++) {
                int iteration = evaluatePoint(col, row, rectangle.getMainWidth(), rectangle.getMainHeight());
                image.setColor(row, col, getColor(iteration));
            }
        }
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

    static int nextX(int x, int y, int x0, int y0, int x1, int y1) {
        int delta = 0;
        if (x < x1 && y == y0) {
            delta = +1;
        } else if (x > x0 && y == y1) {
            delta = -1;
        }
        return x + delta;
    }

    static int nextY(int x, int y, int x0, int y0, int x1, int y1) {
        int delta = 0;
        if (y < y1 && x == x1) {
            delta = +1;
        } else if (y > y0 && x == x0) {
            delta = -1;
        } 
        return y + delta;
    }

    private void saveImage() {
        image.saveToFile("mandelbrot.png");
    }

    public static void main(String[] args) {
        CmdLineOptions opts = CmdLineOptions.makeOrExit(args);

        MarianiSilver ms = new MarianiSilver(opts.width, opts.height);
        Rectangle rectangle = new Rectangle(opts.width, opts.height);
        
        System.out.println("Starting...");
        long time = -System.nanoTime();
        
        //ms.marianiSilver(rectangle);
        List<Rectangle> subRectangles = rectangle.split(INIT_SUBDIV);
        for (Rectangle subr : subRectangles) {
            ms.marianiSilver(subr);
        }
        
        time += System.nanoTime();
        System.out.println("Finished.");
        
        System.out.println("Max Dwell: " + MAX_DWELL + ", Size: "+opts.width+"x"+opts.height
                +", Threshold: "+ RECTANGLE_SIZE_THRESHOLD + ", Split factor: " + SPLIT_FACTOR + ", Max. depth: " + MAX_DEPTH);
        System.out.println("Time: " + Utils.sub("" + time / 1e9, 0, 6) + "s");
        System.out.println("Performance: " + Utils.sub("" + (opts.width*opts.height / (time / 1e3)), 0, 6) + " Mpix/s");
        System.out.println("Evaluated points: " + evaluatedPoints + " (" + ((float)evaluatedPoints*100)/(opts.width*opts.height) +"%)");

        if (SAVE_IMAGE) {
            ms.saveImage();
        }

    }

}
