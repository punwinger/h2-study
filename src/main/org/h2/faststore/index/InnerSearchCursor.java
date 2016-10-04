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
import org.h2.util.New;

import java.util.LinkedList;

//TODO this is loop implement. maybe try stack iterate like page.find() -> leaf.find()
public class InnerSearchCursor {
    static class Info {
        public long pageId;
        public long pageLSN;

        public Info(long pageId, long pageLSN) {
            this.pageId = pageId;
            this.pageLSN = pageLSN;
        }
    }

    private LinkedList<Info> stack = New.linkedList();
    private FSIndex index;
    private FastStore fastStore;

    public InnerSearchCursor(FSIndex index, FastStore fastStore) {
        this.index = index;
        this.fastStore = fastStore;
    }

    // push current page when already latch next page before unlatch current page.
    // because we need to store the correct state(LSN etc.)
    public void pushPage(PageBase page) {
        stack.push(new Info(page.getPageId(), page.getPageLSN()));
    }

    public Info popPage() {
        return stack.pop();
    }

    public boolean isEmpty() {
        return stack.isEmpty();
    }

    public void clear() {
        stack.clear();
    }

    public Info peekPage() {
        return stack.peek();
    }

    //return null while SMO
    // no siblings visit
    public FSLeafPage searchLeaf(Session session, PageBase from, FSRecord record,
                                 boolean checkDuplicate, boolean latchExclusive) {
        PageBase parent = null;
        PageBase child = from == null ? fastStore.getPage(index.getRootPageId()) : from;
        FSLeafPage firstLeaf = null;
        while (true) {
            if (!child.isLeaf()) {
                child.latch(session, false);

                FSRecord maxKey = child.getMinMaxKey(false);
                if (!child.isEmptyPage() &&
                        (index.compareKeys(record, maxKey) <= 0 ||
                                (index.compareKeys(record, maxKey) > 0 && !child.getSMBit()))) {

                    if (parent != null) {
                        pushPage(parent);
                        parent.unlatch(session);
                    }
                    parent = child;

                    long pageID = ((FSNodePage)child).findPage(record, checkDuplicate);
                    child = fastStore.getPage(pageID);
                } else {
                    //encounter page split/delete
                    if (parent != null) {
                        pushPage(parent);
                        parent.unlatch(session);
                    }

                    //TODO remove them
                    child.unlatch(session);
                    //child = traverseBack(session);
                    return null;

                    //may be page will be modified by other thread?
//                    if (child.getPageLSN() != info.pageLSN) {
//                        throw DbException.get(ErrorCode.GENERAL_ERROR_1,
//                                "page " + child.getPageId() + " LSN:" +
//                                        child.getPageLSN() + " != " + info.pageLSN);
//                    }
                }
            } else {
                child.latch(session, latchExclusive);
                if (parent != null) {
                    pushPage(parent);
                    parent.unlatch(session);
                }

                firstLeaf = (FSLeafPage) child;
                break;
            }
        }

        return firstLeaf;
    }

    public PageBase traverseBack(Session session) {
        //we are at point of structural consistency (POSC)
        index.latchForInstant(session, false);

        Info info = null;
        PageBase page = null;

        while (!isEmpty()) {
            info = popPage();
            page = fastStore.getPage(info.pageId);
            if (page.getPageLSN() == info.pageLSN) {
                break;
            }
        }

        return page;
    }

}
