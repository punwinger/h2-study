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
import org.h2.faststore.sync.SXLatch;
import org.h2.faststore.type.FSRecord;
import org.h2.result.SearchRow;

/**
 * TODO
 * 1. insert, delete, update
 * 2. split, merge
 * 3. redo/undo, rollback
 * 4. MVCC
 *
 * 1. array(record, record, record, ...)
 *
 * 2. array(record -> record -> record -> ...) + directory
 * more space(record pointer), less ops during DML, query might slow
 * PageBtree, rows[], offsets[],
 *
 */
public abstract class PageBase {

    private int pageSize;

    protected int entryCount;

    private long pageId;

    private boolean sm_bit;

    protected FSIndex index;

    protected long parentPageId;

    //TODO or something like this
    private long pageLSN;

    private SXLatch latch;

    public PageBase(long pageId, FSIndex index) {
        this.pageId = pageId;
        this.index = index;
        latch = new SXLatch(index.getName() + "-page " + pageId);
    }


    abstract public boolean tryFastAddRow(FSRecord row);
    //return split point
    abstract public long realAddRow(FSRecord row);

    abstract public FSRecord getMinMaxKey(boolean min);

    // bigger  true for the next bigger row, false for the first row = target
    // compareKeys true for compare the row key, for delete

    //add?
    abstract public int binarySearch(FSRecord target, boolean bigger, boolean compareInnerKey);


    abstract public int getMemory();

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getPageLSN() {
        return pageLSN;
    }

    public void setPageLSN(long pageLSN) {
        this.pageLSN = pageLSN;
    }

    public void latch(Session session, boolean exclusive) {
        latch.latch(session, exclusive);
    }

    public void unlatch(Session session) {
        latch.unlatch(session);
    }

    public boolean isEmptyPage() {
        return entryCount == 0;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public boolean getSMBit() {
        return sm_bit;
    }

    public void setSMBit(boolean sm_bit) {
        this.sm_bit = sm_bit;
    }


    public boolean isLeaf() {
        return false;
    }



}
