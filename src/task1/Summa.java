package task1;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Summa {
    public static final int SIZE = 10000000;
    public static final int THREADS = 6;
    public static final int ITEMS_PER_THREAD = SIZE/THREADS;
    static class Acc{
        volatile int acc = 0;
        synchronized public void addToAcc(int n){
            acc += n;
        }
    }

    public static Thread taskThread(int n, int[] schedule, int[] items, int[] results){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            var acc = 0;
            for (int i = start; i < finish; i++) {
                acc += items[i];
            }
            results[n] = acc;
        });
    }
    public static Thread taskMonitorThread(int n, int[] schedule, int[] items, Acc acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.addToAcc(items[i]);
            }
        });
    }
    public static Thread taskAtomicThread(int n, int[] schedule, int[] items, AtomicInteger acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.addAndGet(items[i]);
            }
        });
    }

    public static void measureP(int[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, items, results);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = 0;
        for (int i = 0; i < THREADS; i++) {
            pResult += results[i];
        }
        var pFinish = System.nanoTime();
        System.out.println("Parallel result");
        System.out.println(pResult);
        System.out.println("Parallel time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureMon(int[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, items, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.acc;
        var pFinish = System.nanoTime();
        System.out.println("Monitor result");
        System.out.println(pResult);
        System.out.println("Monitor time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureAtomic(int[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();
        var acc = new AtomicInteger(0);
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, items, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.get();
        var pFinish = System.nanoTime();
        System.out.println("Atomic result");
        System.out.println(pResult);
        System.out.println("Atomic time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void main(String[] args) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }



        var r = new Random();
        int[] ls = new int[SIZE];
        for (int i = 0; i < SIZE; i++) {
            ls[i] = r.nextInt(-99, 100);
        }
        int[] results = new int[THREADS];



        var start = System.nanoTime();
        var acc = 0;
        for (int i = 0; i < SIZE; i++) {
            acc += ls[i];
        }

        var finish = System.nanoTime();

        System.out.println("Sequential result");
        System.out.println(acc);
        System.out.println("Sequential time (ms)");
        System.out.println((double)(finish - start)/1000000);

        measureP(ls);
        measureAtomic(ls);
        measureMon(ls);
    }
}
