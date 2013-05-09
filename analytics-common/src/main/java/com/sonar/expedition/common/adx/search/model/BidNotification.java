// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from sonar.proto

package com.sonar.expedition.common.adx.search.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class BidNotification implements Externalizable, Message<BidNotification> {

    public static Schema<BidNotification> getSchema() {
        return SCHEMA;
    }

    public static BidNotification getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final BidNotification DEFAULT_INSTANCE = new BidNotification();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String id;
    String bidId;
    String impId;
    String seatId;
    String adId;
    String price;
    String currency;
    Long timestamp;

    public BidNotification() {

    }

    public BidNotification(
            String id,
            String bidId,
            Long timestamp
    ) {
        this.id = id;
        this.bidId = bidId;
        this.timestamp = timestamp;
    }

    // getters and setters

    // id

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    // bidId

    public String getBidId() {
        return bidId;
    }

    public void setBidId(String bidId) {
        this.bidId = bidId;
    }

    // impId

    public String getImpId() {
        return impId;
    }

    public void setImpId(String impId) {
        this.impId = impId;
    }

    // seatId

    public String getSeatId() {
        return seatId;
    }

    public void setSeatId(String seatId) {
        this.seatId = seatId;
    }

    // adId

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    // price

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    // currency

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    // timestamp

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<BidNotification> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<BidNotification> SCHEMA = new Schema<BidNotification>() {
        // schema methods

        public BidNotification newMessage() {
            return new BidNotification();
        }

        public Class<BidNotification> typeClass() {
            return BidNotification.class;
        }

        public String messageName() {
            return BidNotification.class.getSimpleName();
        }

        public String messageFullName() {
            return BidNotification.class.getName();
        }

        public boolean isInitialized(BidNotification message) {
            return
                    message.id != null
                            && message.bidId != null
                            && message.timestamp != null;
        }

        public void mergeFrom(Input input, BidNotification message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.id = input.readString();
                        break;
                    case 2:
                        message.bidId = input.readString();
                        break;
                    case 3:
                        message.impId = input.readString();
                        break;
                    case 4:
                        message.seatId = input.readString();
                        break;
                    case 5:
                        message.adId = input.readString();
                        break;
                    case 6:
                        message.price = input.readString();
                        break;
                    case 7:
                        message.currency = input.readString();
                        break;
                    case 8:
                        message.timestamp = input.readInt64();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, BidNotification message) throws IOException {
            if (message.id == null)
                throw new UninitializedMessageException(message);
            output.writeString(1, message.id, false);

            if (message.bidId == null)
                throw new UninitializedMessageException(message);
            output.writeString(2, message.bidId, false);

            if (message.impId != null)
                output.writeString(3, message.impId, false);

            if (message.seatId != null)
                output.writeString(4, message.seatId, false);

            if (message.adId != null)
                output.writeString(5, message.adId, false);

            if (message.price != null)
                output.writeString(6, message.price, false);

            if (message.currency != null)
                output.writeString(7, message.currency, false);

            if (message.timestamp == null)
                throw new UninitializedMessageException(message);
            output.writeInt64(8, message.timestamp, false);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "id";
                case 2:
                    return "bidId";
                case 3:
                    return "impId";
                case 4:
                    return "seatId";
                case 5:
                    return "adId";
                case 6:
                    return "price";
                case 7:
                    return "currency";
                case 8:
                    return "timestamp";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String, Integer> fieldMap = new java.util.HashMap<String, Integer>();

        {
            fieldMap.put("id", 1);
            fieldMap.put("bidId", 2);
            fieldMap.put("impId", 3);
            fieldMap.put("seatId", 4);
            fieldMap.put("adId", 5);
            fieldMap.put("price", 6);
            fieldMap.put("currency", 7);
            fieldMap.put("timestamp", 8);
        }
    };

}