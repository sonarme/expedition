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

public final class BidRequestHolder implements Externalizable, Message<BidRequestHolder> {

    public static Schema<BidRequestHolder> getSchema() {
        return SCHEMA;
    }

    public static BidRequestHolder getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final BidRequestHolder DEFAULT_INSTANCE = new BidRequestHolder();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String userId;
    org.openrtb.BidRequest bidRequest;
    Long timestamp;

    public BidRequestHolder() {

    }

    public BidRequestHolder(
            String userId,
            org.openrtb.BidRequest bidRequest,
            Long timestamp
    ) {
        this.userId = userId;
        this.bidRequest = bidRequest;
        this.timestamp = timestamp;
    }

    // getters and setters

    // userId

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    // bidRequest

    public org.openrtb.BidRequest getBidRequest() {
        return bidRequest;
    }

    public void setBidRequest(org.openrtb.BidRequest bidRequest) {
        this.bidRequest = bidRequest;
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

    public Schema<BidRequestHolder> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<BidRequestHolder> SCHEMA = new Schema<BidRequestHolder>() {
        // schema methods

        public BidRequestHolder newMessage() {
            return new BidRequestHolder();
        }

        public Class<BidRequestHolder> typeClass() {
            return BidRequestHolder.class;
        }

        public String messageName() {
            return BidRequestHolder.class.getSimpleName();
        }

        public String messageFullName() {
            return BidRequestHolder.class.getName();
        }

        public boolean isInitialized(BidRequestHolder message) {
            return
                    message.userId != null
                            && message.bidRequest != null
                            && message.timestamp != null;
        }

        public void mergeFrom(Input input, BidRequestHolder message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.userId = input.readString();
                        break;
                    case 2:
                        message.bidRequest = input.mergeObject(message.bidRequest, org.openrtb.BidRequest.getSchema());
                        break;

                    case 3:
                        message.timestamp = input.readInt64();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, BidRequestHolder message) throws IOException {
            if (message.userId == null)
                throw new UninitializedMessageException(message);
            output.writeString(1, message.userId, false);

            if (message.bidRequest == null)
                throw new UninitializedMessageException(message);
            output.writeObject(2, message.bidRequest, org.openrtb.BidRequest.getSchema(), false);


            if (message.timestamp == null)
                throw new UninitializedMessageException(message);
            output.writeInt64(3, message.timestamp, false);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "userId";
                case 2:
                    return "bidRequest";
                case 3:
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
            fieldMap.put("userId", 1);
            fieldMap.put("bidRequest", 2);
            fieldMap.put("timestamp", 3);
        }
    };

}
