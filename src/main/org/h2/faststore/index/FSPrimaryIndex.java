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


import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.faststore.FSTable;
import org.h2.faststore.FastStore;
import org.h2.faststore.sync.LockBase;
import org.h2.faststore.sync.SXLock;
import org.h2.faststore.type.FSRecord;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;

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
public class FSPrimaryIndex extends FSIndex implements LockBase {
    private SXLock indexLock;

    public FSPrimaryIndex(Database db, FSTable table, FastStore fastStore, int id,
                          IndexColumn[] columns, IndexType indexType) {
        super(table, fastStore);
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
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
    // rowList??  -> add(Session session, RowList list)
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


    // root split
    // keep old root id unchanged and update two new node
    @Override
    public void add(Session session, Row row) {
        //no spilt add
        //indexLock.lock(session, false);

        //check whether need to split
        FSRecord rec = createFSRecord(row);
        FSLeafPage firstLeaf = searchFirstLeaf(session, rec);

        if (firstLeaf.getMemory() + rec.getMemory() >= fastStore.getPageSplitSize()
                && firstLeaf.getEntryCount() > 1) {
            //page split


        }




    }

    private FSLeafPage searchFirstLeaf(Session session, FSRecord rec) {
        InnerSearchCursor innerCursor = new InnerSearchCursor(this, fastStore);
        PageBase from = null;
        while (true) {
            FSLeafPage firstLeaf =  innerCursor.searchLeaf(session, from, rec, true, true);
            if (firstLeaf != null) {
                return firstLeaf;
            } else {
                from = innerCursor.traverseBack(session);
            }
        }
    }

    private void pageSplit(Session session, FSLeafPage firstLeaf) {
        FSLeafPage secondLeaf = (FSLeafPage) fastStore.getPage(firstLeaf.getNextPageId());
        if (secondLeaf != null) {
            fastStore.fixPage(secondLeaf.getPageId());
        }

        latch.latch(session, true);

        long newPageId = fastStore.allocatePage();
        FSLeafPage leafPage = FSLeafPage.create(newPageId, this);
        leafPage.latch(session, true);
        leafPage.setSMBit(true);
        leafPage.setNextPageId(secondLeaf == null ? 0 : secondLeaf.getPageId());
        firstLeaf.setSMBit(true);
        firstLeaf.setNextPageId(newPageId);

        //TODO split point
        int splitPoint = 0;


//
//        if (tryOnly && entryCount > 1) {
//            int x = find(row, false, true, true);
//            if (entryCount < 5) {
//                // required, otherwise the index doesn't work correctly
//                return entryCount / 2;
//            }
//            // split near the insertion point to better fill pages
//            // split in half would be:
//            // return entryCount / 2;
//            int third = entryCount / 3;
//            return x < third ? third : x >= 2 * third ? 2 * third : x;
//        }


    }



    @Override
    public void remove(Session session, Row row) {
        //in-place update ?
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
