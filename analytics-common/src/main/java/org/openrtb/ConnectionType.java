// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from openrtb.proto

package org.openrtb;

public enum ConnectionType implements com.dyuproject.protostuff.EnumLite<ConnectionType> {
    CONNECTION_TYPE_UNKNOWN(0),
    CONNECTION_TYPE_ETHERNET(1),
    CONNECTION_TYPE_WIFI(2),
    CONNECTION_TYPE_CELLULAR_DATA(3),
    CONNECTION_TYPE_CELLULAR_DATA_2G(4),
    CONNECTION_TYPE_CELLULAR_DATA_3G(5),
    CONNECTION_TYPE_CELLULAR_DATA_4G(6);

    public final int number;

    private ConnectionType(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public static ConnectionType valueOf(int number) {
        switch (number) {
            case 0:
                return CONNECTION_TYPE_UNKNOWN;
            case 1:
                return CONNECTION_TYPE_ETHERNET;
            case 2:
                return CONNECTION_TYPE_WIFI;
            case 3:
                return CONNECTION_TYPE_CELLULAR_DATA;
            case 4:
                return CONNECTION_TYPE_CELLULAR_DATA_2G;
            case 5:
                return CONNECTION_TYPE_CELLULAR_DATA_3G;
            case 6:
                return CONNECTION_TYPE_CELLULAR_DATA_4G;
            default:
                return null;
        }
    }
}
