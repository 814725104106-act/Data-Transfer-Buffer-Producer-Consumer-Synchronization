import java.util.concurrent.Semaphore;

/**
 * Data Transfer Buffer - Producer-Consumer Synchronization using Semaphores.
 *  empty : counts free slots   (producer waits when buffer is full)
 *  full  : counts filled slots (consumer waits when buffer is empty)
 *  mutex : mutual exclusion for buffer access
 */
public class ProducerConsumer {

    static final int BUFFER_SIZE = 5;
    static final int TOTAL_ITEMS = 10;

    static final int[] buffer = new int[BUFFER_SIZE];
    static int in = 0, out = 0, count = 0;

    static final Semaphore empty = new Semaphore(BUFFER_SIZE);
    static final Semaphore full  = new Semaphore(0);
    static final Semaphore mutex = new Semaphore(1);

    // Must be called while holding mutex
    static void displayBuffer() {
        StringBuilder sb = new StringBuilder("Buffer : [ ");
        for (int i = 0; i < BUFFER_SIZE; i++) {
            boolean filled = false;
            for (int k = 0; k < count; k++) {
                if ((out + k) % BUFFER_SIZE == i) { filled = true; break; }
            }
            sb.append(filled ? buffer[i] : "_").append(" ");
        }
        sb.append("]  (items = ").append(count).append(")");
        System.out.println(sb);
        System.out.println("---------------------------------------------");
    }

    static class Producer extends Thread {
        public void run() {
            try {
                for (int item = 1; item <= TOTAL_ITEMS; item++) {
                    if (empty.availablePermits() == 0)
                        System.out.println("Producer: Buffer FULL, waiting...");
                    empty.acquire();
                    mutex.acquire();

                    buffer[in] = item;
                    in = (in + 1) % BUFFER_SIZE;
                    count++;
                    System.out.println("Producer produced packet " + item);
                    displayBuffer();

                    mutex.release();
                    full.release();
                    Thread.sleep(300);          // producer is faster
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class Consumer extends Thread {
        public void run() {
            try {
                for (int i = 1; i <= TOTAL_ITEMS; i++) {
                    if (full.availablePermits() == 0)
                        System.out.println("Consumer: Buffer EMPTY, waiting...");
                    full.acquire();
                    mutex.acquire();

                    int item = buffer[out];
                    out = (out + 1) % BUFFER_SIZE;
                    count--;
                    System.out.println("Consumer consumed packet " + item);
                    displayBuffer();

                    mutex.release();
                    empty.release();
                    Thread.sleep(800);          // consumer is slower
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Producer-Consumer using Semaphores (Buffer size = " + BUFFER_SIZE + ")");
        System.out.println("=============================================");
        Producer p = new Producer();
        Consumer c = new Consumer();
        p.start();
        c.start();
        p.join();
        c.join();
        System.out.println("All packets transferred successfully.");
    }
}
