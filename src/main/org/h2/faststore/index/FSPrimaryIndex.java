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
import org.h2.faststore.FastStore;
import org.h2.faststore.sync.LockBase;
import org.h2.faststore.sync.SXLock;
import org.h2.faststore.type.FSRecord;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
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

    @Override
    public void add(Session session, Row row) {
        FSRecord rec = createFSRecord(row);
        InnerSearchCursor innerCursor = new InnerSearchCursor(this, fastStore);
        FSLeafPage leafPage = searchFirstLeaf(session, innerCursor, null, rec, true);
        while (true) {
            leafPage = findValidLeaf(session, leafPage, rec);
            if (leafPage == null) {
                PageBase from = innerCursor.traverseBack(session);
                leafPage = searchFirstLeaf(session, innerCursor, from, rec, true);
                continue;
            }

            int splitPoint = leafPage.checkNeedSplitIfAdd(rec);
            if (splitPoint > 0) {
                //page split will unlatch leafPage
                //maybe latch leafPage again and test if it's valid or not?
                pageSplit(session, leafPage, splitPoint);
                innerCursor.clear();
                leafPage = searchFirstLeaf(session, innerCursor, null, rec, true);
                continue;
            }

            leafPage.addRow(rec, -splitPoint);
            leafPage.unlatch(session);
            break;
        }
        rowCount.incrementAndGet();
    }

    private void pageSplit(Session session, FSLeafPage firstLeaf, int splitPoint) {
        FSLeafPage secondLeaf = (FSLeafPage) fastStore.getPage(firstLeaf.getNextPageId());
        if (secondLeaf != null) {
            fastStore.fixPage(secondLeaf.getPageId());
        }

        //TODO fix all affected pages in buffer pool before latch
        latch.latch(session, true);

        if (secondLeaf != null && secondLeaf.isEmptyPage()) {
            //before acquire index latch. someone free it.
            secondLeaf = (FSLeafPage) fastStore.getPage(firstLeaf.getNextPageId());
        }

        long newPageId = fastStore.allocatePage();
        FSLeafPage leafPage = FSLeafPage.create(newPageId, this);
        leafPage.latch(session, true);
        leafPage.setSMBit(true);
        leafPage.setPrevPageId(firstLeaf.getPageId());

        if (secondLeaf != null) {
            leafPage.setNextPageId(secondLeaf.getPageId());
        }
        firstLeaf.setSMBit(true);
        firstLeaf.setPageLSN(firstLeaf.getPageLSN() + 1);
        firstLeaf.setNextPageId(newPageId);
        FSRecord pivot = null;

        pivot = firstLeaf.splitPage(splitPoint, leafPage);

        if (secondLeaf != null) {
            //need latch?
            secondLeaf.latch(session, true);
            secondLeaf.setPrevPageId(newPageId);
            secondLeaf.unlatch(session);
        }

        //leafPage split is shared, so copy it
        pivot = pivot.copy();

        if (firstLeaf.getPageId() == rootPageId) {
            //root page split
            updateRootPage(session, firstLeaf, pivot, leafPage);
            firstLeaf.setSMBit(false);
            leafPage.setSMBit(false);
            leafPage.unlatch(session);
            firstLeaf.unlatch(session);
        } else {
            FSNodePage parentPage = (FSNodePage) fastStore.getPage(firstLeaf.getParentPageId());
            leafPage.unlatch(session);
            firstLeaf.unlatch(session);
            nodePageSplit(session, parentPage, firstLeaf, pivot, leafPage, leafPage.getMinMaxKey(false));
        }

        latch.unlatch(session);
    }

    //TODO pivot must not be shared...FSNodePage cannot share pivot.
    private void nodePageSplit(Session session, FSNodePage page, PageBase oldChildPage,
                               FSRecord pivot, PageBase newChildPage, FSRecord oldKey) {
        page.latch(session, true);
        page.setPageLSN(page.getPageLSN() + 1);
        int pos = page.checkNeedSplitIfAdd(pivot);
        if (pos > 0) {
            FSNodePage rightPage = splitNode(session, page, pos);

            //split will change oldPage's parentPageId
            if (oldChildPage.getParentPageId() == rightPage.getPageId()) {
                page.unlatch(session);
                page = rightPage;
                page.latch(session, true);
            } else if (oldChildPage.getParentPageId() != page.getPageId()) {
                //cannot find oldChildPage's parent?
                DbException.throwInternalError();
            }
        }

        page.addChild(oldChildPage.getPageId(), pivot, newChildPage.getPageId(), oldKey);
        long newParentPageId = page.getPageId();
        newChildPage.setParentPageId(newParentPageId);
        oldChildPage.setSMBit(false);
        newChildPage.setSMBit(false);
        page.unlatch(session);
    }

    //return:
    //right page that page split to
    private FSNodePage splitNode(Session session, FSNodePage page, int splitPoint) {
        long newPageId = fastStore.allocatePage();
        FSNodePage rightPage = FSNodePage.create(newPageId, this);
        //only after parentPage has added rightPage then it can be visit so no latch for rightPage needed
//            rightPage.latch(session, true);
        rightPage.setSMBit(true);
        page.setSMBit(true);

        FSRecord newPivot = page.splitPage(splitPoint, rightPage);
        FSNodePage parentPage = null;
        if (page.getPageId() == rootPageId) {
            //root split
            updateRootPage(session, page, newPivot, rightPage);
        } else {
            newPivot.setChildPageId(page.getPageId());
            parentPage = (FSNodePage) fastStore.getPage(page.getParentPageId());
            //unlatch before traverse back
            page.unlatch(session);
            nodePageSplit(session, parentPage, page, newPivot,
                    rightPage, rightPage.getMinMaxKey(false));
        }

        return rightPage;
    }


    private FSNodePage updateRootPage(Session session, PageBase oldRootPage,
                                   FSRecord pivot, PageBase rightPage) {
        //root page id remains the same
        long pageId = fastStore.allocatePage();
        //must update cache.
        oldRootPage.setPageId(pageId);
        fastStore.updatePage(oldRootPage);
        FSNodePage newRoot = FSNodePage.create(rootPageId, this);
        newRoot.latch(session, true);
        newRoot.initPage(oldRootPage.getPageId(), pivot, rightPage.getPageId());
        //must updatePage before unlatch see InnerSearchCursor.searchLeaf
        fastStore.updatePage(newRoot);
        newRoot.unlatch(session);

        return newRoot;
    }

    @Override
    public void remove(Session session, Row row) {
        FSRecord rec = createFSRecord(row);
        InnerSearchCursor innerCursor = new InnerSearchCursor(this, fastStore);
        FSLeafPage leafPage = searchFirstLeaf(session, innerCursor, null, rec, true);

        while (true) {
            leafPage = findValidLeaf(session, leafPage, rec);
            if (leafPage != null) {
                break;
            }

            PageBase from = innerCursor.traverseBack(session);
            leafPage = searchFirstLeaf(session, innerCursor, from, rec, true);
        }

        int countBefore = leafPage.getEntryCount();
        FSRecord newLast = leafPage.removeRow(rec);
        if (newLast == null) {
            if (leafPage.getEntryCount() < countBefore) {
                rowCount.decrementAndGet();
            }
            return;
        }

        rowCount.decrementAndGet();
        if (newLast == rec) {
            //set child page id
            pageDelete(session, leafPage, rec);
        } else {
            latch.latch(session, true);
            leafPage.unlatch(session);
            nodePageEntryFix(session, leafPage, rec, newLast);
            latch.unlatch(session);
        }

    }

    private void pageDelete(Session session, FSLeafPage leafPage, FSRecord rec) {
        FSLeafPage prevPage = (FSLeafPage) fastStore.getPage(leafPage.getPrevPageId());
        if (prevPage != null) {
            fastStore.fixPage(prevPage.getPageId());
        }

        FSLeafPage nextPage = (FSLeafPage) fastStore.getPage(leafPage.getNextPageId());
        if (nextPage != null) {
            fastStore.fixPage(nextPage.getPageId());
        }

        latch.latch(session, true);

        if (prevPage != null && prevPage.isEmptyPage()) {
            prevPage = (FSLeafPage) fastStore.getPage(leafPage.getPrevPageId());
        }
        if (nextPage != null && nextPage.isEmptyPage()) {
            nextPage = (FSLeafPage) fastStore.getPage(leafPage.getNextPageId());
        }

        fastStore.freePage(leafPage);
        leafPage.setSMBit(true);

        if (nextPage != null) {
            nextPage.latch(session, true);
            nextPage.setPrevPageId(prevPage == null ?
                    PageBase.INVALID_PAGE_ID : prevPage.getPageId());
            nextPage.unlatch(session);
        }

        leafPage.unlatch(session);
        if (prevPage != null) {
            prevPage.latch(session, true);
            prevPage.setNextPageId(nextPage == null ? PageBase.INVALID_PAGE_ID : nextPage.getPageId());
            prevPage.unlatch(session);
        }

        nodePageEntryRemove(session, leafPage, rec);
        latch.unlatch(session);
    }

    //requestPage.  page which request parent to remove entry. no latch
    private void nodePageEntryRemove(Session session, PageBase requestPage, FSRecord delRecord) {
        if (requestPage.getPageId() == rootPageId) {
            return;
        }

        FSNodePage parentNode = (FSNodePage) fastStore.getPage(requestPage.getParentPageId());
        parentNode.latch(session, true);
        delRecord.setChildPageId(requestPage.getPageId());
        FSRecord last = parentNode.removeRow(delRecord);
        parentNode.setPageLSN(parentNode.getPageLSN() + 1);

        if (last != null) {
            if (last == delRecord) {
                fastStore.freePage(parentNode);
                parentNode.setSMBit(true);
                parentNode.unlatch(session);
                nodePageEntryRemove(session, parentNode, delRecord);
            } else {
                parentNode.unlatch(session);
                nodePageEntryFix(session, parentNode, delRecord, last);
            }
        } else {
            parentNode.unlatch(session);
        }
    }

    //requestPage.  page which request parent to fix entry. no latch.
    private void nodePageEntryFix(Session session, PageBase requestPage,
                                  FSRecord oldMax, FSRecord newMax) {
        if (requestPage.getPageId() == rootPageId) {
            //ignore newMax (maxRecord in B+tree) update
            return;
        }

        while (true) {
            long childPageId = requestPage.getPageId();
            FSNodePage parentNode = (FSNodePage) fastStore.getPage(requestPage.getParentPageId());

            if (parentNode.getMaxRecordChildPageId() == childPageId) {
                //max record in last child page change
                nodePageEntryFix(session, parentNode, oldMax, newMax);
            } else {
                parentNode.latch(session, true);
                //find the record == oldMax in parentNode
                FSRecord parentOldMax = parentNode.findRecord(oldMax, false);
                if (parentOldMax == null) {
                    //must exist in its parent page
                    DbException.throwInternalError();
                }

                int pos = parentNode.allocateNewAndDeallocateOld(parentOldMax, newMax);
                if (pos < 0) {
                    splitNode(session, parentNode, parentNode.getEntryCount() / 2);
                    //split will change requestPage's parent
                    continue;
                }

                parentNode.setPageLSN(parentNode.getPageLSN() + 1);
                parentNode.fixEntryInChild(parentOldMax, newMax);
                parentNode.unlatch(session);
            }

            return;
        }
    }

    private FSLeafPage findValidLeaf(Session session, FSLeafPage leafPage, FSRecord rec) {
        while (leafPage.getNextPageId() != PageBase.INVALID_PAGE_ID
                && compareAllKeys(leafPage.getMinMaxKey(false), rec, false) < 0) {
            //firstLeaf is splitting, insert into secondLeaf
            long nextPageId = leafPage.getNextPageId();
            FSLeafPage nextLeafPage = (FSLeafPage) fastStore.getPage(nextPageId);
            nextLeafPage.latch(session, false);
            leafPage.unlatch(session);
            if (nextLeafPage.isEmptyPage()) {
                nextLeafPage.unlatch(session);
                return null;
            }
            leafPage = nextLeafPage;
        }

        return leafPage;
    }

    private FSLeafPage searchFirstLeaf(Session session, InnerSearchCursor innerCursor, PageBase from,
                                       FSRecord rec, boolean checkDuplicate) {
        while (true) {
            FSLeafPage firstLeaf =  innerCursor.searchLeaf(session, from, rec, checkDuplicate, true);
            if (firstLeaf != null) {
                return firstLeaf;
            } else {
                from = innerCursor.traverseBack(session);
            }
        }
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 0;
    }

    @Override
    public void remove(Session session) {
        //??
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


    @Override
    public int getMaxRecordSize() {
        //TODO get the max record size store in NodePage.  (for SpaceManager allocate)

        return 0;
    }
}
