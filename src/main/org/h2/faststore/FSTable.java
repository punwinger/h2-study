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

package org.h2.faststore;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.faststore.lock.LockBase;
import org.h2.faststore.lock.LockEntity;
import org.h2.faststore.lock.TableLock;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.table.*;
import org.h2.util.New;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.*;

public class FSTable extends TableBase implements LockEntity {
    private ReentrantLock innerlock = new ReentrantLock();

    private LockBase tableLocks;
    private ReadLock readLock;
    private WriteLock writeLock;

    private HashSet<Session> lockShared = New.hashSet();
    private Session lockExclusive = null;

    public FSTable(CreateTableData data) {
        super(data);
        tableLocks = new TableLock(this, true);
        ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = rwl.readLock();
        writeLock = rwl.writeLock();

    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {
        if (!force) {
            exclusive = false;
        }

        innerlock.lock();
        try {
            if (lockExclusive == session) {
                return;
            } else if (lockShared.contains(session)) {
                if (exclusive) {
                    //must release readLock before acquire write lock
                    readLock.unlock();
                    lockShared.remove(session);
                } else {
                    return;
                }
            }
        } finally {
            innerlock.unlock();
        }


        long max = 0;
        //+1 for check dead lock after first tryLock fail
        long sleep = Constants.DEADLOCK_CHECK + 1;
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

            // TODO check dead lock
            // only left time is big enough we check dead lock
            if (sleep > Constants.DEADLOCK_CHECK) {
                ArrayList<Session> sessions = findDeadLock(session, null, null);
                if (sessions != null) {
                    throw DbException.get(
                            ErrorCode.DEADLOCK_1,
                            getDeadlockDetails(sessions));
                }
            }

            long now = System.currentTimeMillis();
            if (now >= max) {
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, getName());
            }

            // don't wait too long so that deadlocks are detected early
            sleep = Math.min(Constants.DEADLOCK_CHECK, max - now);
            if (sleep == 0) {
                sleep = 1;
            }
        }

        innerlock.lock();
        if (exclusive) {
            lockExclusive = session;
        } else {
            lockShared.add(session);
        }
        innerlock.unlock();
    }

    private static String getDeadlockDetails(ArrayList<Session> sessions) {
        // We add the thread details here to make it easier for customers to
        // match up these error messages with their own logs.
        StringBuilder buff = new StringBuilder();
        for (Session s : sessions) {
            LockEntity lock = s.getWaitForEntity();
            buff.append("\nSession ").
                    append(s.toString()).
//                    append(" on thread ").
//                    append(thread.getName()).
                    append(" is waiting to lock ").
                    append(lock.toString()).
                    append(" while locking ");
//            int i = 0;
//            for (Table t : s.getLocks()) {
//                if (i++ > 0) {
//                    buff.append(", ");
//                }
//                buff.append(t.toString());
//                if (t instanceof RegularTable) {
//                    if (((MVTable) t).lockExclusive == s) {
//                        buff.append(" (exclusive)");
//                    } else {
//                        buff.append(" (shared)");
//                    }
//                }
//            }
//            buff.append('.');
        }
        return buff.toString();
    }



    // TODO Async check or ConcurrentHashMap
    @Override
    public ArrayList<Session> findDeadLock(Session session, Session clash, Set<Session> visited) {
        if (clash == null) {
            // verification is started
            clash = session;
            visited = New.hashSet();
        } else if (clash == session) {
            // we found a circle where this session is involved
            return New.arrayList();
        } else if (visited.contains(session)) {
            // we have already checked this session.
            // there is a circle, but the sessions in the circle need to
            // find it out themselves
            return null;
        }
        visited.add(session);
        ArrayList<Session> error = null;

        ArrayList<Session> shares = null;
        //is this assignment ok under multi-thread
        Session exclusive = lockExclusive;

        if (!lockShared.isEmpty()) {
            innerlock.lock();
            try {
                shares = new ArrayList<>(lockShared.size());
                shares.addAll(lockShared);
            } finally {
                innerlock.unlock();
            }
        }


        for (Session s : shares) {
            if (s == session) {
                // it doesn't matter if we have locked the object already
                continue;
            }
            // Index lock?
            LockEntity e = s.getWaitForEntity();
            if (e != null) {
                error = e.findDeadLock(s, clash, visited);
                if (error != null) {
                    error.add(session);
                    break;
                }
            }
        }
        if (error == null && exclusive != null) {
            LockEntity e = exclusive.getWaitForEntity();
            if (e != null) {
                error = e.findDeadLock(exclusive, clash, visited);
                if (error != null) {
                    error.add(session);
                }
            }
        }
        return error;
    }

    @Override
    public void close(Session session) {
        //ignore
    }

    //only invoke one time to unlock all
    @Override
    public void unlock(Session s) {
        //TODO unlock index lock + row lock
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

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
                          boolean create, String indexComment) {
        // 1.scan index is for scan only
        // 2.delegate index is for primary index scan
        // 3.secondary index for other usage
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(
                            ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1,
                            column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            // why lock here? ...
            database.lockMeta(session);
        }

//        MVIndex index;
//        // TODO support in-memory indexes
//        //  if (isPersistIndexes() && indexType.isPersistent()) {
//        int mainIndexColumn;
//        mainIndexColumn = getMainIndexColumn(indexType, cols);
//        if (database.isStarting()) {
//            if (store.store.hasMap("index." + indexId)) {
//                mainIndexColumn = -1;
//            }
//        } else if (primaryIndex.getRowCountMax() != 0) {
//            mainIndexColumn = -1;
//        }
//        if (mainIndexColumn != -1) {
//            primaryIndex.setMainIndexColumn(mainIndexColumn);
//            index = new MVDelegateIndex(this, indexId,
//                    indexName, primaryIndex, indexType);
//        } else if (indexType.isSpatial()) {
//            index = new MVSpatialIndex(session.getDatabase(),
//                    this, indexId,
//                    indexName, cols, indexType);
//        } else {
//            index = new MVSecondaryIndex(session.getDatabase(),
//                    this, indexId,
//                    indexName, cols, indexType);
//        }
//        if (index.needRebuild()) {
//            rebuildIndex(session, index, indexName);
//        }
//        index.setTemporary(isTemporary());
//        if (index.getCreateSQL() != null) {
//            index.setComment(indexComment);
//            if (isSessionTemporary) {
//                session.addLocalTempTableIndex(index);
//            } else {
//                database.addSchemaObject(session, index);
//            }
//        }
//        indexes.add(index);
//        setModified();
//        return index;
    }


    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filter the table filter
     * @param sortOrder the sort order
     * @return the plan item
     */
    //override to remove delegate index dependent.
//    public PlanItem getBestPlanItem(Session session, int[] masks,
//                                    TableFilter filter, SortOrder sortOrder) {
//        PlanItem item = new PlanItem();
//        item.setIndex(getScanIndex(session));
//        item.cost = item.getIndex().getCost(session, null, null, null);
//        ArrayList<Index> indexes = getIndexes();
//        if (indexes != null && masks != null) {
//            for (int i = 1, size = indexes.size(); i < size; i++) {
//                Index index = indexes.get(i);
//                double cost = index.getCost(session, masks, filter, sortOrder);
//                if (cost < item.cost) {
//                    item.cost = cost;
//                    item.setIndex(index);
//                }
//            }
//        }
//        return item;
//    }

    @Override
    public void removeRow(Session session, Row row) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void addRow(Session session, Row row) {

    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {
        return null;
    }

    @Override
    public Index getScanIndex(Session session) {
        return null;
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public long getMaxDataModificationId() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void checkRename() {

    }


}
