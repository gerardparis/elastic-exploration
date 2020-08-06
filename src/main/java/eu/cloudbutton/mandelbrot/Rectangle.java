package eu.cloudbutton.mandelbrot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Rectangle implements Serializable{

    private static final long serialVersionUID = 1L;
    private int mainWidth;
    private int mainHeight;

    private int x0;
    private int y0;
    private int x1;
    private int y1;
    private int borderDwell;
    private int depth;
    private boolean readyToSplit = false; // used in parallel implementation

    public Rectangle(int width, int height, int x0, int y0, int x1, int y1, int depth) {
        this.mainWidth = width;
        this.mainHeight = height;
        this.x0 = x0;
        this.y0 = y0;
        this.x1 = x1;
        this.y1 = y1;
        this.borderDwell = -1;
        this.depth = depth;
    }

    public Rectangle(int width, int height) {
        this(width, height, 0, 0, width - 1, height - 1, 0);
    }

    public int size() {
        return (x1 - x0 + 1) * (y1 - y0 + 1);
    }

    public int getMainWidth() {
        return mainWidth;
    }

    public int getMainHeight() {
        return mainHeight;
    }

    public int getX0() {
        return x0;
    }

    public int getY0() {
        return y0;
    }

    public int getX1() {
        return x1;
    }

    public int getY1() {
        return y1;
    }

    public List<Rectangle> split(int parts) {
        if (parts == 2) {
            return split2();
        } else if (parts == 4) {
            return splitSquare(2);
        } else if (parts == 16) {
            return splitSquare(4);
        } else if (parts == 64) {
            return splitSquare(8);
        } else if (parts == 256) {
                return splitSquare(16);
        } else {
            throw new RuntimeException("Partition not supported");
        }
    }

    private List<Rectangle> split2() {
        List<Rectangle> subRectangles = new ArrayList<>(2);

        int xm = x0 + ((x1 - x0 + 1) / 2);
        int ym = y0 + ((y1 - y0 + 1) / 2);
        
        Rectangle subRectangle1;
        Rectangle subRectangle2;
        if (x1-x0 >= y1-y0) {
            subRectangle1 = new Rectangle(this.mainWidth, this.mainHeight, x0, y0, xm - 1, y1, depth +1);
            subRectangle2 = new Rectangle(this.mainWidth, this.mainHeight, xm, y0, x1, y1, depth +1);
        } else {
            subRectangle1 = new Rectangle(this.mainWidth, this.mainHeight, x0, y0, x1, ym - 1, depth +1);
            subRectangle2 = new Rectangle(this.mainWidth, this.mainHeight, x0, ym, x1, y1, depth +1);
        }

        subRectangles.add(subRectangle1);
        subRectangles.add(subRectangle2);
        return subRectangles;
    }
    
    /**
     * 
     * @param n subdivision factor along each axis
     * @return
     */
    private List<Rectangle> splitSquare(int n) {
        List<Rectangle> subRectangles = new ArrayList<>(n*n);
        
        int xd = ((x1 - x0 + 1) / n);
        int yd = ((y1 - y0 + 1) / n);
        
        int y=y0;
        for (int i=0;i<n;i++) {
            int x=x0;
            for (int j=0;j<n;j++) {
                Rectangle subRectangle = new Rectangle(this.mainWidth, this.mainHeight, x, y, x + xd - 1, y + yd - 1, depth +1);
                subRectangles.add(subRectangle);
                x += xd; 
            }
            y += yd;
        }
        
        return subRectangles;
    }

    public String toString() {
        return "(" + x0 + "," + y0 + ")->(" + x1 + "," + y1 + ") [" + mainWidth + "x" + mainHeight + "] size="
                + size();
    }

    public void setBorderDwell(int borderDwell) {
        this.borderDwell  = borderDwell;
    }
    
    public int getBorderDwell() {
        return borderDwell;
    }

    public void setReadyToSplit(boolean b) {
        readyToSplit  = b;
    }
    
    public boolean isReadyToSplit() {
        return readyToSplit;
    }

    public int getDepth() {
        return depth;
    }
}
