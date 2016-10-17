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

package org.h2.faststore;

public abstract class ArrayObject {
    private static final int COPY_THRESHOLD = 4;
    private static final int DELETE_THRESHOLD = COPY_THRESHOLD + COPY_THRESHOLD / 2;

    //this is much faster (about 2 times) than below two reflect method according to my test.
    //though its still a bit slower than new Row[..]
    public ArrayObject[] removeFromArray(ArrayObject[] old, int oldSize, int pos) {
            ArrayObject[] result;
        if (old.length <= oldSize + DELETE_THRESHOLD) {
            result = old;
        } else {
            result = createArray(oldSize + COPY_THRESHOLD - 1);
            if (pos > 0) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }

        if (oldSize - pos - 1 > 0) {
            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
        }

        return result;
    }

    public ArrayObject[] insertInArray(ArrayObject[] old, int oldSize, int pos, ArrayObject x) {
        ArrayObject[] result;
        if (old != null && old.length > oldSize) {
            result = old;
        } else {
            result = createArray(oldSize + 1 + COPY_THRESHOLD);
            if (pos > 0 && old != null) {
                System.arraycopy(old, 0, result, 0, pos);
            }
        }
        if (old != null && oldSize - pos > 0) {
            System.arraycopy(old, pos, result, pos + 1, oldSize - pos);
        }
        result[pos] = x;
        return result;
    }

    //    @SuppressWarnings("unchecked")
//    public static <T> T[] insertInArray(T[] old, int oldSize, int pos, T x) {
//        T[] result;
//        if (old.length > oldSize) {
//            result = old;
//        } else {
//            // according to a test, this is 2 times slower than "new Row[..]"
//            // use base class to create array[] can improve a bit but still slower than "new Row[..]"
//            result = (T[]) Array.newInstance(
//                    old.getClass().getComponentType(), oldSize + 1 + COPY_THRESHOLD);
//            if (pos > 0) {
//                System.arraycopy(old, 0, result, 0, pos);
//            }
//        }
//        if (oldSize - pos > 0) {
//            System.arraycopy(old, pos, result, pos + 1, oldSize - pos);
//        }
//        result[pos] = x;
//        return result;
//    }
//
//    @SuppressWarnings("unchecked")
//    public static <T> T[] removeFromArray(T[] old, int oldSize, int pos) {
//        T[] result;
//        if (old.length <= oldSize + DELETE_THRESHOLD) {
//            result = old;
//        } else {
//            // according to a test, this is 2 times slower than "new Row[..]"
//            result = (T[]) Array.newInstance(
//                    old.getClass().getComponentType(), oldSize + COPY_THRESHOLD - 1);
//            if (pos > 0) {
//                System.arraycopy(old, 0, result, 0, pos);
//            }
//            //System.arraycopy(old, 0, result, 0, Math.min(oldSize - 1, pos));
//        }
//        if (oldSize - pos - 1 > 0) {
//            System.arraycopy(old, pos + 1, result, pos, oldSize - pos - 1);
//        }
//        return result;
//    }


    protected abstract ArrayObject[] createArray(int sz);
}
