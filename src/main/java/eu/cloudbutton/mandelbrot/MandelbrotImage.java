package eu.cloudbutton.mandelbrot;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

public class MandelbrotImage {
    private static final boolean SHOW_BORDERS = true;

    private int[][] image;

    public void init(int width, int height) {
        image = new int[height][width];
    }

    /**
     * Returns a a two-dimensional int array containing the color values of the
     * Mandelbrot Set, the first dimension is the height and the second dimension is
     * the width.
     * 
     * @return a two-dimensional int array containing the color values of the
     *         Mandelbrot Set.
     */
    public int[][] getImage() {
        return image;
    }

    public void setColor(int row, int col, int color) {
        image[row][col] = color;
    }

    public void setColor(int row, int col, int[][] colorArray) {
        for (int i = 0; i < colorArray.length; i++) {
            for (int j = 0; j < colorArray[i].length; j++) {
                image[row + i][col + j] = colorArray[i][j];
            }
        }
    }

    public void fillColor(Rectangle rectangle, int color) {
        int rowIni = rectangle.getY0();
        int colIni = rectangle.getX0();
        int rowEnd = rectangle.getY1();
        int colEnd = rectangle.getX1();

        for (int i = rowIni; i <= rowEnd; i++) {
            for (int j = colIni; j <= colEnd; j++) {
                image[i][j] = color;
            }
        }
        if (SHOW_BORDERS) {
            // paint borders WHITE
            for (int i = rowIni; i <= rowEnd; i++) {
                image[i][colIni] = Color.WHITE.getRGB();
                image[i][colEnd] = Color.WHITE.getRGB();
            }
            for (int j = colIni; j <= colEnd; j++) {
                image[rowIni][j] = Color.WHITE.getRGB();
                image[rowEnd][j] = Color.WHITE.getRGB();
            }
        }
    }

    public void saveToFile(String filename) {
        int height = image.length;
        int width = image[0].length;

        BufferedImage bufImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);

        for (int row = 0; row < height; row++) {
            for (int col = 0; col < width; col++) {
                bufImage.setRGB(col, row, image[row][col]);
            }
        }

        try {
            ImageIO.write(bufImage, "png", new File(filename));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
