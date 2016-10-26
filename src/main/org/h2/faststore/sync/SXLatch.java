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

package org.h2.faststore.sync;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.util.New;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SXLatch {
    //TODO 改用CAS来进行rw锁的变换？ =0即无锁，>0即为读锁, -1即为写锁?
    private ReentrantReadWriteLock.ReadLock readLock;
    private ReentrantReadWriteLock.WriteLock writeLock;

    private HashSet<Session> lockShared = New.hashSet();
    private Session lockExclusive = null;

    private String name;

    private static final int MAX_LOCK_TIME = 1000;

    public SXLatch(String name) {
        this.name = "Latch " + name;
        ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = rwl.readLock();
        writeLock = rwl.writeLock();
    }

    public void latch(Session session, boolean exclusive) {
        synchronized (this) {
            if (lockExclusive == session) {
                return;
            } else if (lockShared.contains(session)) {
                if (exclusive) {
                    //TODO must upgrade without releasing lock
                    //if two thread upgrade concurrently will be deadlock
                    //use idea like UpdateLock, only one thread hold and holder can read
                    //must release readLock before acquire write lock
                    //needRelease = true;
                    //lockShared.remove(session);

                    DbException.throwInternalError("not implement latch upgrade");
                } else {
                    return;
                }
            }
        }


        doLatch(exclusive, session);

        synchronized (this) {
            if (exclusive) {
                lockExclusive = session;
            } else {
                lockShared.add(session);
            }
        }
    }

    private void doLatch(boolean exclusive, Session session) {
        long maxLockTime = MAX_LOCK_TIME;
        boolean isLatch = false;

        try {
            if (exclusive) {
                isLatch = writeLock.tryLock(maxLockTime, TimeUnit.MILLISECONDS);
            } else {
                isLatch = readLock.tryLock(maxLockTime, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            //ignore
        }

        if (!isLatch) {
            throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, "Latch " + name + " timeout");
        }
    }

    public void unlatch(Session session) {
        boolean exclusive = false;
        synchronized (this) {
            if (lockExclusive == session) {
                exclusive = true;
                lockExclusive = null;
            } else if (lockShared.contains(session)) {
                lockShared.remove(session);
            } else {
                //no lock? something wrong here..
                return;
            }
        }

        if (exclusive) {
            writeLock.unlock();
        } else {
            readLock.unlock();
        }
    }


    @Override
    public String toString() {
        return "latch " + name;
    }
}
