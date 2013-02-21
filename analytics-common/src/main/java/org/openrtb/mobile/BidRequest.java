// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from openrtb.proto

package org.openrtb.mobile;

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

public final class BidRequest implements Externalizable, Message<BidRequest> {

    public static Schema<BidRequest> getSchema() {
        return SCHEMA;
    }

    public static BidRequest getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final BidRequest DEFAULT_INSTANCE = new BidRequest();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String id;
    Integer at;
    Integer tmax;
    List<BidImpression> imp;
    Site site;
    App app;
    Device device;
    User user;
    Restrictions restrictions;

    public BidRequest() {

    }

    public BidRequest(
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

    // at

    public Integer getAt() {
        return at;
    }

    public void setAt(Integer at) {
        this.at = at;
    }

    // tmax

    public Integer getTmax() {
        return tmax;
    }

    public void setTmax(Integer tmax) {
        this.tmax = tmax;
    }

    // imp

    public List<BidImpression> getImpList() {
        return imp;
    }

    public void setImpList(List<BidImpression> imp) {
        this.imp = imp;
    }

    // site

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    // app

    public App getApp() {
        return app;
    }

    public void setApp(App app) {
        this.app = app;
    }

    // device

    public Device getDevice() {
        return device;
    }

    public void setDevice(Device device) {
        this.device = device;
    }

    // user

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    // restrictions

    public Restrictions getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(Restrictions restrictions) {
        this.restrictions = restrictions;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<BidRequest> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<BidRequest> SCHEMA = new Schema<BidRequest>() {
        // schema methods

        public BidRequest newMessage() {
            return new BidRequest();
        }

        public Class<BidRequest> typeClass() {
            return BidRequest.class;
        }

        public String messageName() {
            return BidRequest.class.getSimpleName();
        }

        public String messageFullName() {
            return BidRequest.class.getName();
        }

        public boolean isInitialized(BidRequest message) {
            return
                    message.id != null;
        }

        public void mergeFrom(Input input, BidRequest message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.id = input.readString();
                        break;
                    case 2:
                        message.at = input.readInt32();
                        break;
                    case 3:
                        message.tmax = input.readInt32();
                        break;
                    case 4:
                        if (message.imp == null)
                            message.imp = new ArrayList<BidImpression>();
                        message.imp.add(input.mergeObject(null, BidImpression.getSchema()));
                        break;

                    case 5:
                        message.site = input.mergeObject(message.site, Site.getSchema());
                        break;

                    case 6:
                        message.app = input.mergeObject(message.app, App.getSchema());
                        break;

                    case 7:
                        message.device = input.mergeObject(message.device, Device.getSchema());
                        break;

                    case 8:
                        message.user = input.mergeObject(message.user, User.getSchema());
                        break;

                    case 9:
                        message.restrictions = input.mergeObject(message.restrictions, Restrictions.getSchema());
                        break;

                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, BidRequest message) throws IOException {
            if (message.id == null)
                throw new UninitializedMessageException(message);
            output.writeString(1, message.id, false);

            if (message.at != null)
                output.writeInt32(2, message.at, false);

            if (message.tmax != null)
                output.writeInt32(3, message.tmax, false);

            if (message.imp != null) {
                for (BidImpression imp : message.imp) {
                    if (imp != null)
                        output.writeObject(4, imp, BidImpression.getSchema(), true);
                }
            }


            if (message.site != null)
                output.writeObject(5, message.site, Site.getSchema(), false);


            if (message.app != null)
                output.writeObject(6, message.app, App.getSchema(), false);


            if (message.device != null)
                output.writeObject(7, message.device, Device.getSchema(), false);


            if (message.user != null)
                output.writeObject(8, message.user, User.getSchema(), false);


            if (message.restrictions != null)
                output.writeObject(9, message.restrictions, Restrictions.getSchema(), false);

        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "id";
                case 2:
                    return "at";
                case 3:
                    return "tmax";
                case 4:
                    return "imp";
                case 5:
                    return "site";
                case 6:
                    return "app";
                case 7:
                    return "device";
                case 8:
                    return "user";
                case 9:
                    return "restrictions";
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
            fieldMap.put("at", 2);
            fieldMap.put("tmax", 3);
            fieldMap.put("imp", 4);
            fieldMap.put("site", 5);
            fieldMap.put("app", 6);
            fieldMap.put("device", 7);
            fieldMap.put("user", 8);
            fieldMap.put("restrictions", 9);
        }
    };

}