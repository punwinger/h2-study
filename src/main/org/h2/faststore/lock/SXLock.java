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

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.util.New;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * share/exclusive lock
 */
public class SXLock {
    private ReentrantLock innerlock = new ReentrantLock();

    //TODO 改用CAS来进行rw锁的变换？ =0即无锁，>0即为读锁, -1即为写锁?
    private ReentrantReadWriteLock.ReadLock readLock;
    private ReentrantReadWriteLock.WriteLock writeLock;

    private HashSet<Session> lockShared = New.hashSet();
    private Session lockExclusive = null;

    private String name;

    public SXLock(String name) {
        this.name = name;
        ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = rwl.readLock();
        writeLock = rwl.writeLock();
    }

    public void lock(Session session, boolean exclusive) {
        innerlock.lock();
        try {
            if (lockExclusive == session) {
                return;
            } else if (lockShared.contains(session)) {
                if (exclusive) {
                    //must release readLock before acquire write lock
                    readLock.unlock();
                    lockShared.remove(session);
                    //session.fsSetWaitForLock(null);
                } else {
                    return;
                }
            }
        } finally {
            innerlock.unlock();
        }


        session.fsSetWaitForLock(this);
        try {
            doLock(exclusive, session);
        } finally {
            session.fsSetWaitForLock(null);
        }

        innerlock.lock();
        if (exclusive) {
            lockExclusive = session;
        } else {
            lockShared.add(session);
        }
        innerlock.unlock();
    }

    private void doLock(boolean exclusive, Session session) {
        long max = 0;
        int deadlock_check = Constants.DEADLOCK_CHECK;

        //+1 for check dead lock after first tryLock fail
        long sleep = deadlock_check + 1;
        while (true) {
            try {
                if (exclusive) {
                    if (writeLock.tryLock(sleep, TimeUnit.MILLISECONDS)) {
                        break;
                    }

                } else {
                    if (readLock.tryLock(sleep, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                //ignore
            }

            if (max == 0) {
                max = System.currentTimeMillis() + session.getLockTimeout() - sleep;
            }

            // TODO aysnc check dead lock: wait a short moment and async check
            // only left time is big enough we check dead lock
            if (sleep > deadlock_check) {
                ArrayList<Session> sessions = findDeadLock(session);
                if (sessions != null) {
                    throw DbException.get(ErrorCode.DEADLOCK_1, getDeadlockDetails(sessions));
                }
            }

            long now = System.currentTimeMillis();
            if (now >= max) {
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, name);
            }

            // don't wait too long so that deadlocks are detected early
            sleep = Math.min(deadlock_check, max - now);
            if (sleep == 0) {
                sleep = 1;
            }
        }
    }

    public void unlock(Session s) {
        boolean exclusive = false;
        innerlock.lock();
        try {
            if (lockExclusive == s) {
                exclusive = true;
                lockExclusive = null;
            } else if (lockShared.contains(s)) {
                lockShared.remove(s);
            } else {
                //no lock? something wrong here..
                return;
            }
        } finally {
            innerlock.unlock();
        }

        if (exclusive) {
            writeLock.unlock();
        } else {
            readLock.unlock();
        }
    }

    private static String getDeadlockDetails(ArrayList<Session> sessions) {
        StringBuilder buff = new StringBuilder();
        int idx = 0;
        for (Session s : sessions) {
            SXLock lock = s.fsGetWaitForLock();
            buff.append("Session ").
                    append(s.toString()).
                    append(" -> ").
                    append(lock.toString()).
                    append("(").
                    append(lock.lockExclusive == sessions.get(++idx % sessions.size()) ? "exclusive" : "share").
                    append(" locked) -> ");
        }
        buff.append("Session ").append(sessions.get(0).toString());
        return buff.toString();
    }


    private static class DFSContext {
        private SXLock lock;
        private ArrayList<Session> holder;
        private int index;
        private Session waitSession;

        public DFSContext(SXLock lock, Session waitSession) {
            this.lock = lock;
            this.waitSession = waitSession;
        }

        public Session nextSession() {
            if (holder == null) {
                if (!lock.lockShared.isEmpty()) {
                    lock.innerlock.lock();
                    holder = new ArrayList<>(lock.lockShared.size());
                    holder.addAll(lock.lockShared);
                    lock.innerlock.unlock();
                } else if (lock.lockExclusive != null) {
                    holder = new ArrayList<>(1);
                    holder.add(lock.lockExclusive);
                } else {
                    // other thread may modify the lock...
                    // ignore here
                    // maybe get the innerlock and try again ?
                }
            }

            Session s = null;
            while(holder != null && index < holder.size()) {
                s = holder.get(index++);
                if (s == waitSession) {
                    // It's ok that wait session already get the lock
                    s = null;
                } else {
                    break;
                }
            }

            return s;
        }

        public Session getWaitSession() {
            return waitSession;
        }
    }

    //TODO multi-thread may be problem !!
    private ArrayList<Session> findDeadLock(Session checkSession) {
        ArrayList<Session> res = null;
        HashSet<Session> visited = new HashSet<>();
        LinkedList<DFSContext> stack = new LinkedList<>();

        // need thread sync here
        SXLock lock = checkSession.fsGetWaitForLock();
        DFSContext ctx = new DFSContext(lock, checkSession);
        Session s = ctx.nextSession();
        while (s != null) {
            if (s == checkSession) {
                // find dead lock!
                res = new ArrayList<>();
                for (int i = stack.size() - 1; i >= 0; i--) {
                    res.add(stack.get(i).getWaitSession());
                }
                res.add(ctx.getWaitSession());

                break;
            } else if (!visited.contains(s)) {
                // session not visit yet
                visited.add(s);
                lock = s.fsGetWaitForLock();
                if (lock != null) {
                    stack.push(ctx);
                    ctx = new DFSContext(lock, s);
                }
            } else {
                // find deadlock during search but it must be find by themselves
            }

            // get next session
            s = ctx.nextSession();
            while (s == null && !stack.isEmpty()) {
                ctx = stack.pop();
                s = ctx.nextSession();
            }
        }

        return res;
    }

    @Override
    public String toString() {
        return name;
    }

}
