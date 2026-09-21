package task1;

import java.util.concurrent.atomic.DoubleAdder;

public class Integral {

    public static final int STEPS = 1000000;
    public static final int THREADS = 6;
    public static final int STEPS_PER_THREAD = STEPS / THREADS;

    public static final double A = 0.0;
    public static final double B = 100.0;
    public static final double DX = (B - A) / STEPS;

    public static double f(double x) {
        return Math.exp(-0.05 * x) * (Math.sin(5 * x) + Math.cos(7 * x)) * Math.log10(Math.abs(x) + 2.0);
    }

    static class Acc {
        volatile double acc = 0.0;
        synchronized public void addToAcc(double n) {
            acc += n;
        }
    }

    public static Thread taskThread(int n, int[] schedule, double[] results) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + STEPS_PER_THREAD;
            var acc = 0.0;
            for (int i = start; i < finish; i++) {
                double x = A + DX * (i + 0.5);
                acc += f(x) * DX;
            }
            results[n] = acc;
        });
    }

    public static Thread taskMonitorThread(int n, int[] schedule, Acc acc) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + STEPS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + DX * (i + 0.5);
                acc.addToAcc(f(x) * DX);
            }
        });
    }

    public static Thread taskAtomicThread(int n, int[] schedule, DoubleAdder acc) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + STEPS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + DX * (i + 0.5);
                acc.add(f(x) * DX);
            }
        });
    }

    public static void measureP() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * STEPS_PER_THREAD;
        }

        double[] results = new double[THREADS];
        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, results);
        }

        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        var pResult = 0.0;
        for (int i = 0; i < THREADS; i++) {
            pResult += results[i];
        }

        var pFinish = System.nanoTime();
        System.out.println("Parallel result");
        System.out.println(pResult);
        System.out.println("Parallel time (ms)");
        System.out.println((double)(pFinish - pStart) / 1000000);
    }

    public static void measureMon() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * STEPS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        Acc acc = new Acc();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        var pResult = acc.acc;
        var pFinish = System.nanoTime();
        System.out.println("Monitor result");
        System.out.println(pResult);
        System.out.println("Monitor time (ms)");
        System.out.println((double)(pFinish - pStart) / 1000000);
    }

    public static void measureAtomic() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * STEPS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        var acc = new DoubleAdder();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        var pResult = acc.sum();
        var pFinish = System.nanoTime();
        System.out.println("Atomic result");
        System.out.println(pResult);
        System.out.println("Atomic time (ms)");
        System.out.println((double)(pFinish - pStart) / 1000000);
    }

    public static void main(String[] args) throws InterruptedException {
        var start = System.nanoTime();
        double seqResult = 0.0;

        for (int i = 0; i < STEPS; i++) {
            double x = A + DX * (i + 0.5);
            seqResult += f(x) * DX;
        }
        var finish = System.nanoTime();

        System.out.println("Sequential result");
        System.out.println(seqResult);
        System.out.println("Sequential time (ms)");
        System.out.println((double)(finish - start) / 1000000);

        measureP();
        measureAtomic();
        measureMon();
    }
}