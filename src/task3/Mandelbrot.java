package task3;

public class Mandelbrot {

    public static final int WIDTH = 120;
    public static final int HEIGHT = 40;
    public static final int MAX_ITER = 500;
    public static final int THREADS = 6;

    public static final String PALETTE = " .:-=+*#%@";

    private static int countIterations(double cReal, double cImag) {
        double zReal = 0.0;
        double zImag = 0.0;
        int iter = 0;

        while (zReal * zReal + zImag * zImag <= 4.0 && iter < MAX_ITER) {
            double nextZReal = zReal * zReal - zImag * zImag + cReal;
            zImag = 2.0 * zReal * zImag + cImag;
            zReal = nextZReal;
            iter++;
        }
        return iter;
    }

    public static Thread taskThread(int n, int[] threadsStart, char[] pixels) {
        return new Thread(() -> {
            int startY = threadsStart[n];
            int rowsPerThread = HEIGHT / THREADS;
            int endY = startY + rowsPerThread;

            if (n == THREADS - 1) {
                endY = HEIGHT;
            }

            for (int y = startY; y < endY; y++) {
                for (int x = 0; x < WIDTH; x++) {

                    double cReal = (x - WIDTH / 2.0) * 3.2 / WIDTH - 0.5;
                    double cImag = (y - HEIGHT / 2.0) * 3.2 / HEIGHT * 0.5;

                    int iter = countIterations(cReal, cImag);

                    if (iter == MAX_ITER) {
                        pixels[y * WIDTH + x] = ' ';
                    } else {
                        pixels[y * WIDTH + x] = PALETTE.charAt(iter % PALETTE.length());
                    }
                }
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        int rowsPerThread = HEIGHT / THREADS;

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * rowsPerThread;
        }

        char[] pixels = new char[WIDTH * HEIGHT];
        var start = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, pixels);
        }

        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        var finish = System.nanoTime();

        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < HEIGHT; y++) {
            for (int x = 0; x < WIDTH; x++) {
                sb.append(pixels[y * WIDTH + x]);
            }
            sb.append('\n');
        }

        System.out.print(sb);
        System.out.println("Parallel time (ms): " + (double)(finish - start) / 1000000);
    }
}