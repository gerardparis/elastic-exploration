package eu.cloudbutton.mandelbrot;

import java.awt.Color;
import java.io.Serializable;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;

public class MarianiSilverRunnable implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int BLACK = Color.BLACK.getRGB();

    private static int[] colors;

    static {
        colors = new int[MarianiSilverParallel.MAX_DWELL];
        for (int i = 0; i < MarianiSilverParallel.MAX_DWELL; i++) {
            colors[i] = Color.HSBtoRGB(i / 256f, 1, i / (i + 8f)); // brownish
        }
    }

    private Rectangle rectangle;
    private MandelbrotImage image;
    private Deque<Rectangle> queue;

    public MarianiSilverRunnable(Rectangle rectangle, MandelbrotImage image, Deque<Rectangle> queue) {
        this.rectangle = rectangle;
        this.image = image;
        this.queue = queue;
    }

    @Override
    public void run() {
        if (borderHasCommonDwell(rectangle)) {
            // System.out.println("Computing border...");
            image.fillColor(rectangle, getColor(rectangle.getBorderDwell()));

        } else if (rectangle.getDepth() >= MarianiSilverParallel.MAX_DEPTH 
                || rectangle.size() <= MarianiSilverParallel.RECTANGLE_SIZE_THRESHOLD) {
            // per-pixel evaluation of the rectangle
            // System.out.println("Computing per-pixel...");
            evaluateAndSet(rectangle);

        } else {
            rectangle.setReadyToSplit(true);
            // System.out.println("Splitting...");
            // main thread will split it.

        }
        queue.offer(rectangle);
        //return null;
    }

    private static int getColor(int dwell) {
        if (dwell < MarianiSilverParallel.MAX_DWELL)
            return colors[dwell];
        else
            return BLACK;
    }

    private int evaluatePoint(int col, int row, int width, int height) {
        double x0 = (col - width / 2) * 4.0 / width;
        double y0 = (row - height / 2) * 4.0 / width;
        double x = 0, y = 0;
        int iteration = 0;
        while (x * x + y * y < 4 && iteration < MarianiSilverParallel.MAX_DWELL) {
            double xTemp = x * x - y * y + x0;
            y = 2 * x * y + y0;
            x = xTemp;
            iteration++;
        }
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

}
