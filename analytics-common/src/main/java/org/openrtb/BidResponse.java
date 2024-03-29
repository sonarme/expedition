// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from openrtb.proto

package org.openrtb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class BidResponse implements Externalizable, Message<BidResponse> {

    public static Schema<BidResponse> getSchema() {
        return SCHEMA;
    }

    public static BidResponse getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final BidResponse DEFAULT_INSTANCE = new BidResponse();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String id;
    List<SeatBid> seatbid;
    String bidid;
    String cur;
    String customdata;

    public BidResponse() {

    }

    public BidResponse(
            String id
    ) {
        this.id = id;
    }

    // getters and setters

    // id

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    // seatbid

    public List<SeatBid> getSeatbidList() {
        return seatbid;
    }

    public void setSeatbidList(List<SeatBid> seatbid) {
        this.seatbid = seatbid;
    }

    // bidid

    public String getBidid() {
        return bidid;
    }

    public void setBidid(String bidid) {
        this.bidid = bidid;
    }

    // cur

    public String getCur() {
        return cur;
    }

    public void setCur(String cur) {
        this.cur = cur;
    }

    // customdata

    public String getCustomdata() {
        return customdata;
    }

    public void setCustomdata(String customdata) {
        this.customdata = customdata;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<BidResponse> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<BidResponse> SCHEMA = new Schema<BidResponse>() {
        // schema methods

        public BidResponse newMessage() {
            return new BidResponse();
        }

        public Class<BidResponse> typeClass() {
            return BidResponse.class;
        }

        public String messageName() {
            return BidResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return BidResponse.class.getName();
        }

        public boolean isInitialized(BidResponse message) {
            return
                    message.id != null;
        }

        public void mergeFrom(Input input, BidResponse message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.id = input.readString();
                        break;
                    case 2:
                        if (message.seatbid == null)
                            message.seatbid = new ArrayList<SeatBid>();
                        message.seatbid.add(input.mergeObject(null, SeatBid.getSchema()));
                        break;

                    case 3:
                        message.bidid = input.readString();
                        break;
                    case 4:
                        message.cur = input.readString();
                        break;
                    case 5:
                        message.customdata = input.readString();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, BidResponse message) throws IOException {
            if (message.id == null)
                throw new UninitializedMessageException(message);
            output.writeString(1, message.id, false);

            if (message.seatbid != null) {
                for (SeatBid seatbid : message.seatbid) {
                    if (seatbid != null)
                        output.writeObject(2, seatbid, SeatBid.getSchema(), true);
                }
            }


            if (message.bidid != null)
                output.writeString(3, message.bidid, false);

            if (message.cur != null)
                output.writeString(4, message.cur, false);

            if (message.customdata != null)
                output.writeString(5, message.customdata, false);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "id";
                case 2:
                    return "seatbid";
                case 3:
                    return "bidid";
                case 4:
                    return "cur";
                case 5:
                    return "customdata";
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
            fieldMap.put("seatbid", 2);
            fieldMap.put("bidid", 3);
            fieldMap.put("cur", 4);
            fieldMap.put("customdata", 5);
        }
    };

}
