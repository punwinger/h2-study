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
import org.h2.faststore.FastStore;
import org.h2.faststore.type.FSRecord;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;


//TODO
//1.MVCC
//2.how to handle traverseBack better when push many leaf pages
public class FetchCursor implements Cursor {
    private InnerSearchCursor innerCursor;
    private Session session;
    private FastStore fastStore;
    private FSIndex index;

    private FSRecord current;
    private Row currentRow;


    private FSLeafPage currentLeafPage;
    private long currentLeafPageLSN;

    private FSRecord max;
    private FSRecord min;

    public FetchCursor(Session session, FSIndex index, InnerSearchCursor innerCursor,
                        FastStore fastStore, FSRecord min, FSRecord max) {
        this.session = session;
        this.index = index;
        this.innerCursor = innerCursor;
        this.fastStore = fastStore;

        //min.getKey == Long.MIN_VALUE for easy to find the smallest one >= cmpRecord
        this.min = min;
        //min.getKey == Long.MAX_VALUE
        this.max = max;
    }

    @Override
    public Row get() {
        if (currentRow == null && current != null) {
            currentRow = index.getRow(session, current.getKey());
        }
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return current;
    }

    @Override
    public boolean next() {
        if (currentLeafPage == null) {
            return false;
        }

        if (current != null) {
            if (!fetchNext()) {
                PageBase from = innerCursor.traverseBack(session);
                searchAndFetchLeaf(from, current);
            }
        }

        //make sure no holding any latch here
        if (current == null) {
            currentLeafPage = null;
            return false;
        }

        return true;
    }

    //find the record >= compare
    public boolean fetch(FSLeafPage firstLeaf, FSRecord compare) {
        FSLeafPage leaf = firstLeaf;
        FSLeafPage nextLeaf = null;
        FSRecord candidate = null;

        //leaf already latch
        if (leaf.isEmptyPage()) {
            leaf.unlatch(session);
            return false;
        }

        //find the next record >= compare
        candidate = leaf.findRecord(compare, false);

        while (candidate == null) {
            nextLeaf = (FSLeafPage) fastStore.getPage(leaf.getNextPageId());
            if (nextLeaf == null) {
                //we are over the rightmost leaf
                break;
            }

            if (nextLeaf.isEmptyPage()) {
                // need traverse back
                innerCursor.pushPage(leaf);
                leaf.unlatch(session);
                return false;
            }

            nextLeaf.latch(session, false);
            //check again after latch to make sure it's not empty
            if (nextLeaf.isEmptyPage()) {
                // need traverse back
                innerCursor.pushPage(leaf);
                nextLeaf.unlatch(session);
                leaf.unlatch(session);
                return false;
            }

            candidate = nextLeaf.findRecord(compare, false);

            innerCursor.pushPage(leaf);
            leaf.unlatch(session);
            leaf = nextLeaf;
        }

        leaf.unlatch(session);
        current = candidate;
        currentLeafPage = leaf;
        currentLeafPageLSN = currentLeafPage.getPageLSN();
        return true;
    }

    //TODO MVCC will not work?
    //1.lsn is invalid  ->  return false
    //2.leaf page is rightmost   -> return true
    //3.current > max   -> return true
    private boolean fetchNext() {
        currentLeafPage.latch(session, false);

        //TODO too strict?
        if (currentLeafPage.getPageLSN() != currentLeafPageLSN) {
            return false;
        }

        FSRecord record = current.getNext();
        if (record == null) {
            FSLeafPage nextPage = (FSLeafPage) fastStore.getPage(
                    currentLeafPage.getNextPageId());
            if (nextPage != null) {
                if (nextPage.isEmptyPage()) {
                    //next page is under page delete
                    innerCursor.pushPage(currentLeafPage);
                    currentLeafPage.unlatch(session);
                    return false;
                } else {
                    nextPage.latch(session, false);
                    //double check
                    if (nextPage.isEmptyPage()) {
                        innerCursor.pushPage(currentLeafPage);
                        nextPage.unlatch(session);
                        currentLeafPage.unlatch(session);
                        return false;
                    }

                    record = nextPage.getMinMaxKey(true);
                    currentLeafPage.unlatch(session);
                    currentLeafPage = nextPage;
                }
            } else {
                //next page null means its the last record
            }
        }

        current = record;
        if (current != null && index.compareKeys(current, max) > 0) {
            current = null;
        }

        currentLeafPage.unlatch(session);
        return true;
    }

    // find the next record >= target
    private FSRecord findNextRecord(FSRecord from, FSRecord target) {
        while (from != null && index.compareKeys(from, target) < 0) {
            from = from.getNext();
        }
        return from;
    }

    void searchAndFetchLeaf(PageBase from, FSRecord record) {
        while (true) {
            FSLeafPage firstLeaf = innerCursor.searchLeaf(session, from, record, false, false);
            if (firstLeaf == null) {
                from = innerCursor.traverseBack(session);
                continue;
            }

            if (fetch(firstLeaf, record)) {
                break;
            } else {
                from = innerCursor.traverseBack(session);
            }
        }
    }

    @Override
    public boolean previous() {
        //...
        //avoid dead latch

        return false;
    }
}
