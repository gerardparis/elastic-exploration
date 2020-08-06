package eu.cloudbutton.mandelbrot.serverless;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class PlotData {
    
    public static Font getTitleFont() {
        return new Font("Dialog", Font.BOLD, 16);
    }
    
    public static void plotConcurrency(List<TaskStats> taskStatsList) {
        XYSeriesCollection dataset1 = new XYSeriesCollection();

        // (finishTime,duration) series
        long maxFinishTime = 0L;
        List<Map.Entry<Long, Long>> startFinishTimes = new ArrayList<>();
        for (TaskStats stats : taskStatsList) {
            long finishTime = stats.getEndTime();
            maxFinishTime = Math.max(maxFinishTime, finishTime);

            startFinishTimes.add(Map.entry(stats.getInitTime(), finishTime));
        }
        
        // Sort series list
        startFinishTimes.sort(new Comparator<Map.Entry<Long, Long>>() {
            
            @Override
            public int compare(Map.Entry<Long, Long> o1, Map.Entry<Long, Long> o2) {
                return Long.compare(o1.getKey(), o2.getKey());
            }
            
        });
        // Add series to dataset
        int j=0;
        for (Map.Entry<Long, Long> e : startFinishTimes) {
            XYSeries duration = new XYSeries(j);
            duration.add((double)e.getKey(), j);
            duration.add((double)e.getValue(), j);
            dataset1.addSeries(duration);
            j++;
        }
        
        XYSeries concurrencySeries = new XYSeries("");
        int bins = 2000;
        for(double i = 0.0; i < maxFinishTime; i+=((double)maxFinishTime)/bins) {
            int count = 0;

            for (TaskStats stats : taskStatsList) {
                long finishTime = stats.getEndTime();
                if (i > stats.getInitTime() && i < finishTime) {
                    count++;
                }
                
            }
            
            concurrencySeries.add(i, count);
        }
        
        XYSeriesCollection dataset2 = new XYSeriesCollection();
        dataset2.addSeries(concurrencySeries);
        
        //dataset.addSeries(lineCollectionSeries);
        
        String plotTitle = "Concurrency";
        String xaxis = "Time";
        String yaxis1 = "Task id";
        String yaxis2 = "Concurrent tasks";
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        boolean show = false;
        boolean toolTips = false;
        boolean urls = false;
        
        
        //construct the plot
        XYPlot plot = new XYPlot();
        plot.setDataset(0, dataset1);
        plot.setDataset(1, dataset2);

        //customize the plot with renderers and axis
        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer();
        for(int k=0;k<taskStatsList.size();k++) {
            renderer1.setSeriesPaint(k, Color.BLUE);
            renderer1.setSeriesShapesVisible(k, false);
        }
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer();
        renderer2.setSeriesShapesVisible(0, false);
        plot.setRenderer(0, renderer1);
        plot.setRenderer(1, renderer2);
        plot.setRangeAxis(0, new NumberAxis(yaxis1));
        plot.setRangeAxis(1, new NumberAxis(yaxis2));
        plot.setDomainAxis(new NumberAxis(xaxis));

        //Map the data to the appropriate axis
        plot.mapDatasetToRangeAxis(0, 0);
        plot.mapDatasetToRangeAxis(1, 1);   

        //generate the chart
        JFreeChart chart = new JFreeChart(plotTitle, getTitleFont(), plot, false);
        chart.setBackgroundPaint(Color.WHITE);
        
        
        
        
        //JFreeChart chart = ChartFactory.createXYLineChart(
        //        plotTitle, xaxis, yaxis, dataset, orientation, show, toolTips, urls);
        
        /*XYPlot plot = (XYPlot)chart.getPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        
        for(int k=0;k<finishTimes.size();k++) {
            renderer.setSeriesPaint(k, Color.BLUE);
            renderer.setSeriesShapesVisible(k, false);
        }
        renderer.setSeriesShapesVisible(finishTimes.size(), false);
        plot.setRenderer(renderer);*/
        
        int width = 1000;
        int height = 600;
        try {
            ChartUtils.saveChartAsPNG(new File("concurrency.PNG"), chart, width, height);
        } catch (IOException e) {
        }
    }


    public static void plotConcurrencyHybrid(List<TaskStats> taskStatsListOrig) {
        XYSeriesCollection dataset1 = new XYSeriesCollection();

        List<TaskStats> taskStatsList = new ArrayList<>(taskStatsListOrig);

        // (finishTime,duration) series
        long maxFinishTime = 0L;

        for (TaskStats stats : taskStatsList) {
            long finishTime = stats.getEndTime();
            maxFinishTime = Math.max(maxFinishTime, finishTime);
        }

        // Sort series list
        taskStatsList.sort(new Comparator<TaskStats>() {

            @Override
            public int compare(TaskStats o1, TaskStats o2) {
                return Long.compare(o1.getInitTime(), o2.getInitTime());
            }

        });
        // Add series to dataset
        int j=0;
        for (TaskStats e : taskStatsList) {
            XYSeries duration = new XYSeries(j);
            duration.add((double)e.getInitTime(), j);
            duration.add((double)e.getEndTime(), j);
            dataset1.addSeries(duration);
            j++;
        }

        XYSeries concurrencySeries = new XYSeries("");
        int bins = 2000;
        for(double i = 0.0; i < maxFinishTime; i+=((double)maxFinishTime)/bins) {
            int count = 0;

            for (TaskStats stats : taskStatsList) {
                long finishTime = stats.getEndTime();
                if (i > stats.getInitTime() && i < finishTime) {
                    count++;
                }

            }

            concurrencySeries.add(i, count);
        }

        XYSeriesCollection dataset2 = new XYSeriesCollection();
        dataset2.addSeries(concurrencySeries);

        //dataset.addSeries(lineCollectionSeries);

        String plotTitle = "Concurrency";
        String xaxis = "Time";
        String yaxis1 = "Task id";
        String yaxis2 = "Concurrent tasks";
        PlotOrientation orientation = PlotOrientation.VERTICAL;
        boolean show = false;
        boolean toolTips = false;
        boolean urls = false;


        //construct the plot
        XYPlot plot = new XYPlot();
        plot.setDataset(0, dataset1);
        plot.setDataset(1, dataset2);

        //customize the plot with renderers and axis
        XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer();
        for(int k=0;k<taskStatsList.size();k++) {
            renderer1.setSeriesPaint(k, taskStatsList.get(k).getExecType() == ExecType.REMOTE ? Color.BLUE : Color.ORANGE);
            renderer1.setSeriesShapesVisible(k, false);
        }
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer();
        renderer2.setSeriesShapesVisible(0, false);
        plot.setRenderer(0, renderer1);
        plot.setRenderer(1, renderer2);
        plot.setRangeAxis(0, new NumberAxis(yaxis1));
        plot.setRangeAxis(1, new NumberAxis(yaxis2));
        plot.setDomainAxis(new NumberAxis(xaxis));

        //Map the data to the appropriate axis
        plot.mapDatasetToRangeAxis(0, 0);
        plot.mapDatasetToRangeAxis(1, 1);

        //generate the chart
        JFreeChart chart = new JFreeChart(plotTitle, getTitleFont(), plot, false);
        chart.setBackgroundPaint(Color.WHITE);

        int width = 1000;
        int height = 600;
        try {
            ChartUtils.saveChartAsPNG(new File("concurrency.PNG"), chart, width, height);
        } catch (IOException e) {
        }
    }
}
