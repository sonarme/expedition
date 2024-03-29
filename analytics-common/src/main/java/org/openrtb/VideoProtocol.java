// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from openrtb.proto

package org.openrtb;

public enum VideoProtocol implements com.dyuproject.protostuff.EnumLite<VideoProtocol> {
    VIDEO_PROTOCOL_VAST_10(1),
    VIDEO_PROTOCOL_VAST_20(2),
    VIDEO_PROTOCOL_VAST_30(3),
    VIDEO_PROTOCOL_VAST_10_WRAPPER(4),
    VIDEO_PROTOCOL_VAST_20_WRAPPER(5),
    VIDEO_PROTOCOL_VAST_30_WRAPPER(6);

    public final int number;

    private VideoProtocol(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public static VideoProtocol valueOf(int number) {
        switch (number) {
            case 1:
                return VIDEO_PROTOCOL_VAST_10;
            case 2:
                return VIDEO_PROTOCOL_VAST_20;
            case 3:
                return VIDEO_PROTOCOL_VAST_30;
            case 4:
                return VIDEO_PROTOCOL_VAST_10_WRAPPER;
            case 5:
                return VIDEO_PROTOCOL_VAST_20_WRAPPER;
            case 6:
                return VIDEO_PROTOCOL_VAST_30_WRAPPER;
            default:
                return null;
        }
    }
}
