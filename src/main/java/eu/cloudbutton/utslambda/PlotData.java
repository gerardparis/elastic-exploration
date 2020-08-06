package eu.cloudbutton.utslambda;

import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class PlotData {
    
    public static void plotConcurrency(List<Map.Entry<Long,Long>> finishTimes) {
        XYSeriesCollection dataset = new XYSeriesCollection();

        // (finishTime,duration) series
        XYSeries series1 = new XYSeries("");
        long maxFinishTime = 0L;
        for (Map.Entry<Long, Long> e : finishTimes) {
            maxFinishTime = Math.max(maxFinishTime, e.getKey());
        }
        int bins = 2000;
        for(double i = 0.0; i < maxFinishTime; i+=((double)maxFinishTime)/bins) {
            int count = 0;
            
            for (Map.Entry<Long, Long> e : finishTimes) {
                if (i>e.getKey()-e.getValue() && i < e.getKey()) {
                    count++;
                }
                
            }
            
            series1.add(i, count);
        }
        
        dataset.addSeries(series1);
        
        String plotTitle = "Concurrency";
        String xaxis = "Time";
        String yaxis = "Concurrent tasks";
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        boolean show = false;
        boolean toolTips = false;
        boolean urls = false;
        JFreeChart chart = ChartFactory.createXYLineChart(
                plotTitle, xaxis, yaxis, dataset, orientation, show, toolTips, urls);
        
        int width = 1000;
        int height = 600;
        try {
            ChartUtils.saveChartAsPNG(new File("concurrency.PNG"), chart, width, height);
        } catch (IOException e) {
        }
    }

    public static void plotScatterDuration(List<Map.Entry<Long,Long>> finishTimes) {
        XYSeriesCollection dataset = new XYSeriesCollection();

        // (finishTime,duration) series
        XYSeries series1 = new XYSeries("");
        for (Map.Entry<Long, Long> e : finishTimes) {
            series1.add(e.getKey(), e.getValue());
        }
        dataset.addSeries(series1);
        
        String plotTitle = "Task duration over time";
        String xaxis = "Time";
        String yaxis = "Task duration";
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        boolean show = false;
        boolean toolTips = false;
        boolean urls = false;
        JFreeChart chart = ChartFactory.createScatterPlot(
                plotTitle, xaxis, yaxis, dataset, orientation, show, toolTips, urls);
        
        Shape shape  = new Ellipse2D.Double(0,0,2,2);
        XYPlot xyPlot = (XYPlot) chart.getPlot();
        XYItemRenderer renderer = xyPlot.getRenderer();
        renderer.setSeriesShape(0, shape);
        
        int width = 1000;
        int height = 600;
        try {
            ChartUtils.saveChartAsPNG(new File("scatter.PNG"), chart, width, height);
        } catch (IOException e) {
        }
    }
    
    public static void plotHistogram(List<Long> durations) {

        double[] values = durations.stream().mapToDouble(l -> l).toArray();

        int number = 10;
        HistogramDataset dataset = new HistogramDataset();
        dataset.setType(HistogramType.RELATIVE_FREQUENCY);
        dataset.addSeries("Histogram", values, number);
        String plotTitle = "Histogram";
        String xaxis = "task duration";
        String yaxis = "fraction";
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        boolean show = false;
        boolean toolTips = false;
        boolean urls = false;
        JFreeChart chart = ChartFactory.createHistogram(plotTitle, xaxis, yaxis, dataset, orientation, show, toolTips,
                urls);
        int width = 500;
        int height = 300;
        try {
            ChartUtils.saveChartAsPNG(new File("histogram.PNG"), chart, width, height);
        } catch (IOException e) {
        }
    }

    public static void main(String[] args) {
        List<Long> list = new ArrayList<>();
        list.add(1L);
        list.add(1L);
        list.add(1L);
        list.add(2L);
        list.add(2L);
        list.add(3L);
        plotHistogram(list);
        //plotScatterDuration(list, list);
    }
}
