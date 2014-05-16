/*
 * ┌──┐
 * │  │
 * │Eh│ony
 * └──┘
 */
package org.ehony.camel;

import org.apache.camel.Route;
import org.apache.camel.impl.RoutePolicySupport;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.camel.util.ObjectHelper.*;
import static org.apache.commons.lang.Validate.isTrue;

public class SerialAccessRoutePolicy extends RoutePolicySupport
{

    private Lock lock;
    private List<Route> routes = new CopyOnWriteArrayList<Route>();
    private ExecutorService pool;
    private long delay = 1000;
    
    /**
     * Create new serial access Camel route policy which stores
     * lock monitoring task in internal thread pool.
     * 
     * @param lock re-entrant lock to synchronise on.
     */
    public SerialAccessRoutePolicy(Lock lock) {
        this(lock, newFixedThreadPool(1));
    }

    /**
     * Create new serial access Camel route policy which stores
     * lock monitoring task in provided thread pool.
     * <p>Provided lock <font color="red">must be re-entrant</font> in order
     * to support sequential lock status check.</p>
     * 
     * @param lock re-entrant lock to synchronise on.
     * @param pool thread pool to keep lock monitor.
     */
    public SerialAccessRoutePolicy(Lock lock, ExecutorService pool) {
        notNull(lock, "Expected lock.");
        notNull(pool, "Expected thread pool.");
        this.lock = lock;
        this.pool = pool;
        
        // Auto-start lock monitor.
        pool.submit(new LockSpy());
    }
    
    public long getLockObtainDelay() {
        return delay;
    }

    /**
     * Set interval between lock obtain retries. 
     * @param delay positive delay.
     */
    public void setLockObtainDelay(long delay) {
        isTrue(delay > 0, "Expected positive lock obtain delay.");
        this.delay = delay;
    }

    @Override
    public void onInit(final Route route) {
        route.getRouteContext().getRoute().noAutoStartup();
        routes.add(route);
    }

    public void onRemove(Route route) {
        routes.remove(route);
        if (routes.isEmpty() && lock.tryLock()) {
            lock.unlock();
        }
    }
    
    private class LockSpy implements Runnable {

        @Override
        public synchronized void run() {
            while (true) {
                try {
                    if (routes.isEmpty()) {
                        routes.wait();
                    } else {
                        if (lock.tryLock()) { 
                            for (Route route : routes) {
                                route.getRouteContext().getRoute().setAutoStartup("true");
                                route.getRouteContext().getCamelContext().startRoute(route.getId());
                            }
                        } else {
                            for (Route route : routes) {
                                route.getRouteContext().getCamelContext().stopRoute(route.getId());
                            }
                        }
                        sleep(delay);
                    }
                } catch (Exception e) {
                    try {
                        lock.unlock();
                    } finally {
                        // Prevent death of monitoring thread.
                        pool.submit(new LockSpy());
                    }
                    throw wrapRuntimeCamelException(e);
                }
            }
        }
    }
}
