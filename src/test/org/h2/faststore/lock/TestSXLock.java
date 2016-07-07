/*
 * Copyright (c) 2016. Pun.W <punyj177 at gmail dot com>
 *
 * Everyone is permitted to copy and distribute verbatim or modified copies of this license
 * document, and changing it is allowed as long as the name is changed.
 *
 * DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE TERMS AND CONDITIONS FOR COPYING,
 * DISTRIBUTION AND MODIFICATION
 *
 * 0. You just DO WHAT THE FUCK YOU WANT TO.
 */

package org.h2.faststore.lock;

import org.h2.engine.Session;
import org.junit.Test;

public class TestSXLock {

    private static class TSession extends Session {
        private String tName;

        public TSession(String name) {
            this.tName = name;
        }

        public int getLockTimeout() {
            return 5000;
        }

        public void lock(SXLock lock, boolean exclusive) {
            lock.lock(this, exclusive);
        }

        @Override
        public String toString() {
            return tName;
        }
    }

    @Test
    public void testFindDeadlock() {
        final TSession s1 = new TSession("s1");
        final TSession s2 = new TSession("s2");
        final TSession s3 = new TSession("s3");
        final TSession s4 = new TSession("s4");
        final TSession s5 = new TSession("s5");
        final TSession s6 = new TSession("s6");


        //S
        final SXLock l1 = new SXLock("l1");

        //X
        final SXLock l2 = new SXLock("l2");

        //S
        final SXLock l3 = new SXLock("l3");

        final int lockWaitTime = 600;
        final int threadKeepTime = 3000;


        //complex dead lock situation...
        new Thread(new Runnable() {
            @Override
            public void run() {
                s1.lock(l1, false);
                sleeNoException(lockWaitTime);
                s1.lock(l3, true);
                sleeNoException(threadKeepTime);
            }
        }, "s1").start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                s2.lock(l1, false);
                sleeNoException(threadKeepTime);
            }
        }, "s2").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                s4.lock(l2, true);
                sleeNoException(lockWaitTime);
                s4.lock(l3, true);
                sleeNoException(threadKeepTime);
            }
        }, "s4").start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                s5.lock(l3, false);
                sleeNoException(lockWaitTime);
                s5.lock(l1, true);
                sleeNoException(threadKeepTime);
            }
        }, "s5").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                s6.lock(l3, false);
                sleeNoException(lockWaitTime);
                s6.lock(l1, true);
                sleeNoException(threadKeepTime);
            }
        }, "s6").start();


        s3.lock(l1, false);
        sleeNoException(lockWaitTime);
        s3.lock(l2, false);
        sleeNoException(threadKeepTime);
    }

    private static void sleeNoException(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
