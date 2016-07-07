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
import org.h2.engine.Session;
import org.h2.faststore.lock.LockBase;
import org.h2.faststore.lock.SXLock;
import org.h2.index.BaseIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.table.*;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class FSTable extends TableBase implements LockBase {
    private ReentrantLock innerlock = new ReentrantLock();
    private SXLock tableLock;


    public FSTable(CreateTableData data) {
        super(data);
        tableLock = new SXLock(getName());
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {
        if (!force) {
            exclusive = false;
        }
        lockSession(session, exclusive);
    }

    @Override
    public void lockSession(Session session, boolean exclusive) {
        tableLock.lock(session, exclusive);
        session.fsAddLocks(this);
    }

    @Override
    public void unlockSession(Session s) {
        tableLock.unlock(s);
    }

    @Override
    public void close(Session session) {
        //ignore
    }

    //only invoke one time to unlock all
    @Override
    public void unlock(Session s) {
        //ignore
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

        BaseIndex index = null;
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
        return index;
    }


    /**
     * Get the best plan for the given search mask.
     *
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
