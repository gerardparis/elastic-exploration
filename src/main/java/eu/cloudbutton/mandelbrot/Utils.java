package eu.cloudbutton.mandelbrot;

public class Utils {
    public static String sub(final String str, final int start, final int end) {
        return str.substring(start, Math.min(end, str.length()));
    }
}
