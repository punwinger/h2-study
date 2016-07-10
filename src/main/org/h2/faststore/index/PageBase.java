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
abstract class PageBase {

    private int pageSize;

    private int entryCount;

    private long pageId;

    protected FSIndex index;


    //leaf
    //array(record -> record -> record -> ...) + directory
    private FSRecord[] directory;
    private FSRecord[] records;

    //node
    //array(record, record, record, ...)
    private FSRecord[] keys;
    private long[] childPageIds;



    public PageBase(FSIndex index) {
        this.index = index;
    }


    abstract public boolean tryFastAddRow(FSRecord row);

    //return split point
    abstract public long realAddRow(FSRecord row);



    // only for node page
    public int binarySearch(FSRecord target) {
        int l = 0, r = entryCount - 1;
        int m = 0;
        while (l <= r) {
            m = (l + r) >>> 1;
            int cmp = index.compareRecord(keys[m], target);
            if (cmp == 0) {
                return m;
            } else if (cmp > 0) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        return -(l + 1);
    }



    public boolean isLeaf() {
        return false;
    }


}
