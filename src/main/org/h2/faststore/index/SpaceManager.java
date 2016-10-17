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

import org.h2.faststore.ArrayObject;
import org.h2.faststore.Util;
import org.h2.message.DbException;

//TODO SpaceManager是否compact. compact由于一般需要记录整个页的数据，会大大增加redo/undo日志大小
class SpaceManager {
    private static class SpacePos extends ArrayObject {
        public int left;  //include
        public int right;  //include

        public static SpacePos INSTANCE = new SpacePos(-1, -1);

        public SpacePos(int l, int r) {
            left = l;
            right = r;
        }

        public int getSize() {
            return right - left + 1;
        }

        @Override
        public String toString() {
            return "Pos l:" + left + " r:" + right;
        }

        @Override
        protected ArrayObject[] createArray(int sz) {
            return new SpacePos[sz];
        }
    }

    private SpacePos[] poses;
    private int count;

    private int leftSize;
    private int minLeftSize;

    public SpaceManager(int left, int right, int mlSize) {
        poses = (SpacePos[]) SpacePos.INSTANCE.insertInArray(poses,
                0, 0, new SpacePos(left, right));
        count = 1;
        leftSize = right - left + 1;
        minLeftSize = mlSize;
    }

    public SpaceManager(int size, int minLeftSize) {
        this(0, size - 1, minLeftSize);
    }

    public SpaceManager(int size) {
        this(0, size - 1, 0);
    }

    // O(N)
    //TODO better algorithm in O(logN)?
    public int tryAllocate(int size) {
        if (minLeftSize > 0 && leftSize - size < minLeftSize) {
            return -1;
        }

        for (int i = 0; i < count; ++i) {
            int sz = poses[i].getSize();
            if (sz >= size) {
                leftSize -= size;
                return i;
            }
        }
        return -1;
    }

    public int allocate(int pos, int size) {
        if (pos < 0 || pos >= count) {
            return -1;
        }

        int offset = -1;
        int sz = poses[pos].getSize();
        if (sz == size) {
            offset = poses[pos].left;
            poses = (SpacePos[]) SpacePos.INSTANCE.removeFromArray(poses, count, pos);
            count--;
        } else if (sz > size) {
            offset = poses[pos].left;
            poses[pos].left += size;
        }

        return offset;
    }

    // O(logN)
    public void deallocate(int offset, int size) {
        leftSize += size;

        int l = 0, r = count - 1;
        while (l <= r) {
            int m = (l + r) >>> 1;
            if (poses[m].left == offset) {
                DbException.throwInternalError(
                        "offset overlap free space while deallocate. " + toString());
            } else if (poses[m].left > offset) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        //l is the smallest one bigger than offset
        boolean hasMerge = false;
        //merge into right space
        if (l < count && offset + size == poses[l].left) {
            poses[l].left = offset;
            hasMerge = true;
        }

        //merge into left space
        if (l > 0 && poses[l - 1].right + 1 == offset) {
            if (hasMerge) {
                poses[l - 1].right = poses[l].right;
                poses = (SpacePos[]) SpacePos.INSTANCE.removeFromArray(poses, count, l);
                count--;
                return;
            }

            poses[l - 1].right += size;
            hasMerge = true;
        }

        if (!hasMerge) {
            poses = (SpacePos[]) SpacePos.INSTANCE.insertInArray(
                    poses, count, l, new SpacePos(offset, offset + size - 1));
            count++;
        }
    }

    @Override
    public String toString() {
        return "[Space Detail: count:" + count + " " + Util.array2String(poses, count) + "]";
    }
}
