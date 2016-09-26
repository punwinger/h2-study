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
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.util.New;
import org.h2.value.Value;

import java.util.HashMap;
import java.util.LinkedList;

abstract public class FSIndex extends BaseIndex implements LockBase {

    protected FSTable table;

    protected FastStore fastStore;

    protected SXLatch latch;

    //long for support large file
    private long rootPageId;


    public FSIndex(FSTable table, FastStore fastStore) {
        this.table = table;
        this.fastStore = fastStore;
        latch = new SXLatch(getName());
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
        FSRecord min = createFSRecord(first);
        FSRecord max = createFSRecord(last);

        return new FetchCursor(session, this, innerSearchCursor, fastStore, min, max);
    }

    protected FSRecord createFSRecord(SearchRow r) {
        FSRecord rec = null;
        if (r != null) {
            rec = new FSRecord(r.getColumnCount());
            rec.setKey(r.getKey());
            for (int i = 0; i < columns.length; i++) {
                Column c = columns[i];
                int idx = c.getColumnId();
                Value v = r.getValue(idx);
                rec.setValue(idx, v);
            }
        }

        return rec;
    }


    public long getRootPageId() {
        return rootPageId;
    }

    public void setRootPageId(long rootPageId) {
        this.rootPageId = rootPageId;
    }
}
