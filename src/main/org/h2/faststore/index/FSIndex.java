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

import org.h2.engine.Session;
import org.h2.faststore.FSTable;
import org.h2.faststore.FastStore;
import org.h2.faststore.sync.LockBase;
import org.h2.faststore.sync.SXLatch;
import org.h2.faststore.type.FSRecord;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.New;
import org.h2.value.Value;

import java.util.HashMap;
import java.util.LinkedList;

abstract public class FSIndex extends BaseIndex implements LockBase {

    protected FSTable table;

    protected FastStore fastStore;

    protected SXLatch latch;

    //long for support large file
    protected long rootPageId;

    private Column[] storeColumns;


    public FSIndex(FSTable table, FastStore fastStore) {
        this.table = table;
        this.fastStore = fastStore;
        latch = new SXLatch(getName());
    }

    @Override
    protected void initBaseIndex(Table newTable, int id, String name,
                                 IndexColumn[] newIndexColumns, IndexType newIndexType) {
        super.initBaseIndex(newTable, id, name, newIndexColumns, newIndexType);

        //TODO init storeColumns (store columns in leaf. primary/secondary index is different)
    }


    // NOTE: only compare index columns(keys)
    public int compareKeys(FSRecord a, FSRecord b) {
        return a.compare(b, columnIds, indexColumns, table.getCompareMode());
    }

    public int compareInnerKey(FSRecord a, FSRecord b) {
        long aKey = a.getKey();
        long bKey = b.getKey();
        return aKey == bKey ? 0 : (aKey > bKey ? 1 : -1);
    }

    public int compareAllKeys(FSRecord a, FSRecord b, boolean checkDuplicate) {
        int c = compareKeys(a, b);
        if (c == 0) {
            if (checkDuplicate && getIndexType().isUnique()) {
                if (!containsNullAndAllowMultipleNull(b)) {
                    throw getDuplicateKeyException(b.toString());
                }
            }
            c = compareInnerKey(a, b);
        }

        return c;
    }

    public void latchForInstant(Session session, boolean exclusive) {
        latch.latch(session, exclusive);
        latch.unlatch(session);
    }

    @Override
    public Row getRow(Session session, long key) {
        return null;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        InnerSearchCursor innerSearchCursor = new InnerSearchCursor(this, fastStore);
        FSRecord min = createFSRecord(first, Long.MIN_VALUE);
        FSRecord max = createFSRecord(last, Long.MAX_VALUE);

        FetchCursor cursor = new FetchCursor(session, this, innerSearchCursor, fastStore, min, max);
        cursor.searchAndFetchLeaf(null, min);
        return cursor;
    }

    protected FSRecord createFSRecord(SearchRow r, long innerKey) {
        FSRecord rec = null;
        if (r != null) {
            rec = new FSRecord(r.getColumnCount());
            rec.setKey(innerKey);
            for (int i = 0; i < columns.length; i++) {
                Column c = columns[i];
                int idx = c.getColumnId();
                Value v = r.getValue(idx);
                rec.setValue(idx, v);
            }
        }

        return rec;
    }

    public int getRecordIndexSize(Data dummy, FSRecord row) {
        return getRowSize(dummy, row, columns, false);
    }

    public int getRecordSize(Data dummy, FSRecord row) {
        return getRowSize(dummy, row, storeColumns, false);
    }

    private int getRowSize(Data dummy, FSRecord row, Column[] cs, boolean onlyPosition) {
        int rowsize = Data.getVarLongLen(row.getKey());
        if (row.getNext() != null) {
            rowsize += Data.getVarLongLen(row.getNext().getOffset());
        }

        //TODO need onlyPosition?
        //if (!onlyPosition) {
        for (Column col : cs) {
            Value v = row.getValue(col.getColumnId());
            rowsize += dummy.getValueLen(v);
        }
        // }
        return rowsize;
    }

    protected FSRecord createFSRecord(SearchRow r) {
        return createFSRecord(r, r.getKey());
    }

    @Override
    public boolean containsNullAndAllowMultipleNull(SearchRow newRow) {
        return super.containsNullAndAllowMultipleNull(newRow);
    }

    @Override
    public DbException getDuplicateKeyException(String key) {
        return super.getDuplicateKeyException(key);
    }

    public long getRootPageId() {
        return rootPageId;
    }

    public void setRootPageId(long rootPageId) {
        this.rootPageId = rootPageId;
    }

    public FastStore getStore() {
        return fastStore;
    }

    abstract public int getMaxRecordSize();
}
