package eu.cloudbutton.mandelbrot.serverless;

import java.io.Serializable;

import eu.cloudbutton.mandelbrot.Rectangle;

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
    private long initTs;
    private long endTs;
    private ExecType execType;
    
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

    public long getInitTs() {
        return initTs;
    }

    public void setInitTs(long initTs) {
        this.initTs = initTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public void setEndTs(long endTs) {
        this.endTs = endTs;
    }

    public ExecType getExecType() {
        return execType;
    }

    public void setExecType(ExecType execType) {
        this.execType = execType;
    }
}
