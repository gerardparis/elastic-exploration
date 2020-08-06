package eu.cloudbutton.mandelbrot.parallelns;

import eu.cloudbutton.mandelbrot.Rectangle;

import java.io.Serializable;

public class Result implements Serializable{
    
    private static final long serialVersionUID = 1L;
    
    enum Action {
        FILL, SPLIT, SET_DWELL_ARRAY
    }
    
    private int dwellToFill;
    private Action nextAction;
    private int[][] dwellArray;
    private int xArray;
    private int yArray;
    private Rectangle rectangle;
    
    public Result(Rectangle rectangle) {
        this.rectangle = rectangle;
    }
    
    public Rectangle getRectangle() {
        return rectangle;
    }

    public void setNextAction(Action nextAction) {
        this.nextAction = nextAction;
    }
    
    public void setDwellToFill(int dwellToFill) {
        this.dwellToFill = dwellToFill;
    }

    public int getDwellToFill() {
        return dwellToFill;
    }

    public Action getNextAction() {
        return nextAction;
    }
    
    public void setDwellArray(int[][] dwellArray) {
        this.dwellArray = dwellArray;
    }

    public int[][] getDwellArray() {
        return dwellArray;
    }

    public int getxArray() {
        return xArray;
    }

    public int getyArray() {
        return yArray;
    }
    

}
