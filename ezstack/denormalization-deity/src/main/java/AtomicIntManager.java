import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIntManager {

    private AtomicInteger _integer;

    public AtomicIntManager(AtomicInteger integer) {
        _integer = integer;
    }

    public void increment() {
        _integer.getAndIncrement();
    }

    public int getAndZero() {
        return _integer.getAndSet(0);
    }
}
