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

import org.h2.faststore.Util;
import org.h2.faststore.type.FSRecord;
import org.h2.message.DbException;

//TODO handle overflow data in primary index.
public class FSLeafPage extends PageBase {
    //redo & undo just need to write the change part...

    //entryCount
//    private FSRecord[] records;
//    private int[] offsets;

    private long nextPageId = INVALID_PAGE_ID;

    private long prevPageId = INVALID_PAGE_ID;

    public FSLeafPage(long pageId, FSIndex index, int minDirectoryCount,
                      int maxDirectoryCount, int pageSize) {
        super(pageId, index, pageSize, minDirectoryCount, maxDirectoryCount);
    }

    public static FSLeafPage create(long pageId, FSIndex index) {
        return new FSLeafPage(pageId, index, MIN_DIRECTORY_NUM,
                MAX_DIRECTORY_NUM, PAGE_SIZE);
    }

    @Override
    public FSRecord splitPage(int splitPoint, PageBase newPage) {
        if (splitPoint >= entryCount || splitPoint <= 0) {
            DbException.throwInternalError(
                    "split point " + splitPoint + " invalid. entryCount:" + entryCount);
        }

        FSRecord iter = directories[0].start;
        //todo read all record
        reset();

        FSRecord pivot = null, add = null;

        //TODO handle overflow record.
        for (int i = 0; i < splitPoint; i++) {
            //addRow will adjust setNext.
            add = iter;
            iter = iter.getNext();
            addRowInner(add);
        }
        pivot = add;

        while (iter != null) {
            add = iter;
            iter = iter.getNext();
            newPage.addRowInner(add);
        }

        return pivot;
    }

    @Override
    public int getMemory() {
        return 0;
    }

    @Override
    public void remapChildren() {
        //no child page
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    public long getNextPageId() {
        return nextPageId;
    }

    public void setNextPageId(long nextPageId) {
        this.nextPageId = nextPageId;
    }

    public long getPrevPageId() {
        return prevPageId;
    }

    public void setPrevPageId(long prevPageId) {
        this.prevPageId = prevPageId;
    }

    @Override
    public String toString() {
        return "entryCount:" + entryCount
                + " directoryCount:" + directoryCount + " directories:"
                + Util.array2String(directories, directoryCount) + " " + spaceManager.toString();
    }
}
