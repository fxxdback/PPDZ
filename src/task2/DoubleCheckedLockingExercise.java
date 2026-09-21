package task2;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * ДОПОЛНИТЕЛЬНАЯ ЗАДАЧА. Double-Checked Locking
 *
 * Объект ExpensiveResource должен создаваться лениво: только при первом вызове
 * getInstance(). После создания все потоки должны получать один и тот же,
 * полностью сконструированный экземпляр.
 *
 * Реализуйте LazyResource.getInstance() по схеме Double-Checked Locking:
 *
 *   1) первая проверка instance выполняется без synchronized;
 *   2) если instance отсутствует, поток входит в synchronized (monitor);
 *   3) внутри synchronized instance проверяется ещё раз;
 *   4) только после второй проверки создаётся ExpensiveResource;
 *   5) поле instance должно иметь модификатор, обеспечивающий корректную
 *      публикацию полностью сконструированного объекта другим потокам.
 *
 * Синхронизировать весь метод getInstance нельзя: после инициализации быстрый
 * путь не должен захватывать monitor.
 *
 * Вопрос после выполнения: зачем нужны обе проверки и почему одного
 * synchronized-блока недостаточно без правильного объявления instance?
 */
public class DoubleCheckedLockingExercise {
    private static final class ExpensiveResource {
        private static final AtomicInteger CONSTRUCTIONS = new AtomicInteger();
        private int[] data;
        private long checksum;

        private ExpensiveResource() {
            CONSTRUCTIONS.incrementAndGet();

            data = new int[10_000];
            long calculatedChecksum = 0;
            for (int i = 0; i < data.length; i++) {
                data[i] = i * 31 + 17;
                calculatedChecksum += data[i];
            }
            checksum = calculatedChecksum;
        }

        boolean isValid() {
            if (data == null || data.length != 10_000) {
                return false;
            }

            long calculatedChecksum = 0;
            for (int value : data) {
                calculatedChecksum += value;
            }
            return calculatedChecksum == checksum;
        }
    }

    private static final class LazyResource {
        private final Object monitor = new Object();

        private volatile ExpensiveResource instance;

        ExpensiveResource getInstance() {
            ExpensiveResource localRef = instance;

            if(localRef == null) {
                synchronized (monitor) {
                    localRef = instance;
                    if(localRef == null) {
                        instance = localRef = new ExpensiveResource();
                    }
                }
            }
            return localRef;
        }
    }

    public static void main(String[] args) throws Exception {
        int rounds = 100;
        int callersPerRound = 32;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        ExpensiveResource.CONSTRUCTIONS.set(0);

        try {
            for (int round = 0; round < rounds; round++) {
                LazyResource lazy = new LazyResource();
                Set<ExpensiveResource> instances = new HashSet<>();
                @SuppressWarnings("unchecked")
                Future<ExpensiveResource>[] results = new Future[callersPerRound];

                for (int caller = 0; caller < callersPerRound; caller++) {
                    results[caller] = pool.submit(lazy::getInstance);
                }

                for (Future<ExpensiveResource> result : results) {
                    ExpensiveResource instance;
                    try {
                        instance = result.get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        throw new AssertionError("The DCL test did not finish", e);
                    }

                    if (!instance.isValid()) {
                        throw new AssertionError(
                                "A thread observed a partially initialized resource");
                    }
                    instances.add(instance);
                }

                if (instances.size() != 1) {
                    throw new AssertionError(
                            "Round " + round + " created " + instances.size()
                                    + " different instances");
                }
            }
        } finally {
            pool.shutdownNow();
        }

        int constructions = ExpensiveResource.CONSTRUCTIONS.get();
        if (constructions != rounds) {
            throw new AssertionError(
                    "Expected " + rounds + " constructor calls, got " + constructions);
        }

        System.out.println("OK: one fully initialized instance per lazy holder");
    }

    /*
     * Вывод, означающий, что задача, скорее всего, решена правильно:
     * OK: one fully initialized instance per lazy holder
     *
     * Важно: даже многократный успешный запуск не доказывает, что решение без
     * volatile корректно. Допустимое поведение задаёт Java Memory Model, а
     * конкретное запрещённое наблюдение может ни разу не проявиться в тесте.
     */
}

    /*
    * Вопрос после выполнения:
    *первая проверка исключает избыточные блокировки потоков, а вторая гарантирует,
    *что объект будет создан только первым вошедшим потоком.
    *Одного synchronized-блока недостаточно, тк первая проверка выполняется вне этого блока.
    *без volatile запись адреса памяти в instance может выполниться до выполнения конструктора объекта.
     * */

    /*
    * Задание:
    *В обоих случаях нет, тк полный порядок не дает появиться взаимной блокировке.
    *Например, поток2 не сможет взять ресурс2, пока у него нет ресурса1.
    *И поэтому вообще не важно в каком порядке они освобождаются.
    * */