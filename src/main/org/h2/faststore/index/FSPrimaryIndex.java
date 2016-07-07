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

package org.h2.faststore.index;


import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.faststore.FSTable;
import org.h2.faststore.lock.LockBase;
import org.h2.faststore.lock.SXLock;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * TODO
 * 1. index lock + row lock
 * 2. data struct for better concurrent ops(add, update, delete), page & leaf?
 * 3. split & merge
 * 4. MVCC
 * 5. in-place update
 * 6. undo/redo
 *
 */
public class FSPrimaryIndex extends BaseIndex implements LockBase {
    private FSTable table;
    private SXLock indexLock;

    public FSPrimaryIndex(Database db, FSTable table, int id,
                          IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        this.table = table;
        indexLock = new SXLock(getName());
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void lockSession(Session session, boolean exclusive) {
        indexLock.lock(session, exclusive);
        session.fsAddLocks(this);
    }

    @Override
    public void unlockSession(Session s) {
        indexLock.unlock(s);
    }

    // add & split
    // rowList??
    // add(no split):
    // 1.index share lock
    // 2.binary search to find the leaf page
    // 3. enough empty space ? if true exclusive lock the leaf page else add(split)
    // 4. enough empty space ? if true add record to the empty space, point to next record(MVCC info)
    //      else release lock add(split)
    // 5. fix the pointer of prev record
    // 6. m_own > 8 -> fix pointer in directory(thread-safe? copy to local?)

    // add(split):
    // 1.index exclusive lock
    // 2.binary search find the leaf page
    // 3.enough space? if true exclusive lock leaf page add(no split) return;
    // 4.split leaf page (split point @ 1/3, 2/3)
    // take care of row lock
    // 5. to be continued...
    @Override
    public void add(Session session, Row row) {


    }

    @Override
    public void remove(Session session, Row row) {
        //in-place update ?
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        // 1.index share lock
        // 2.No leaf page share lock, keep thread safe

        return null;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 0;
    }

    @Override
    public void remove(Session session) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return null;
    }

    @Override
    public boolean needRebuild() {
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


}
