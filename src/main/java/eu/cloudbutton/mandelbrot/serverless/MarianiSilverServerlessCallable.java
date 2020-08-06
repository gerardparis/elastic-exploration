package eu.cloudbutton.mandelbrot.serverless;

import java.io.Serializable;
import java.util.concurrent.Callable;

import eu.cloudbutton.mandelbrot.Rectangle;

public class MarianiSilverServerlessCallable implements Callable<Result>, Serializable {

    private static final long serialVersionUID = 1L;
    
    private Rectangle rectangle;

    public MarianiSilverServerlessCallable(Rectangle rectangle) {
        this.rectangle = rectangle;
    }

    @Override
    public Result call() {
        long init = System.currentTimeMillis();
        Result result = new Result(rectangle);
        if (borderHasCommonDwell(rectangle)) {
            // System.out.println("Computing border...");
            result.setNextAction(Result.Action.FILL);
            result.setDwellToFill(rectangle.getBorderDwell());
            //image.fillColor(rectangle, getColor(rectangle.getBorderDwell()));

        } else if (rectangle.getDepth() >= MarianiSilverServerless.MAX_DEPTH 
                || rectangle.size() <= MarianiSilverServerless.RECTANGLE_SIZE_THRESHOLD) {
            // per-pixel evaluation of the rectangle
            // System.out.println("Computing per-pixel...");
            int[][] dwellArray = evaluate(rectangle);
            result.setNextAction(Result.Action.SET_DWELL_ARRAY);
            result.setDwellArray(dwellArray);

        } else {
            result.setNextAction(Result.Action.SPLIT);
            //rectangle.setReadyToSplit(true);
            // System.out.println("Splitting...");
            // main thread will split it.

        }
        //queue.offer(rectangle);
        long end = System.currentTimeMillis();
        result.setInitTs(init);
        result.setEndTs(end);

        if (System.getProperty("hybrid") != null) {
            result.setExecType(ExecType.LOCAL);
        } else {
            result.setExecType(ExecType.REMOTE);
        }

        return result;
    }

    

    private int evaluatePoint(int col, int row, int width, int height) {
        double x0 = (col - width / 2) * 4.0 / width;
        double y0 = (row - height / 2) * 4.0 / width;
        double x = 0, y = 0;
        int iteration = 0;
        while (x * x + y * y < 4 && iteration < MarianiSilverServerless.MAX_DWELL) {
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
