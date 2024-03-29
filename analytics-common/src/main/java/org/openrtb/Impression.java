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

public final class Impression implements Externalizable, Message<Impression> {

    public static Schema<Impression> getSchema() {
        return SCHEMA;
    }

    public static Impression getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final Impression DEFAULT_INSTANCE = new Impression();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String id;
    Banner banner;
    Video video;
    String displaymanager;
    String displaymanagerver;
    Bool instl;
    String tagid;
    Float bidfloor;
    String bidfloorcur;
    List<String> iframebuster;
    ImpressionExt ext;

    public Impression() {

    }

    public Impression(
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

    // banner

    public Banner getBanner() {
        return banner;
    }

    public void setBanner(Banner banner) {
        this.banner = banner;
    }

    // video

    public Video getVideo() {
        return video;
    }

    public void setVideo(Video video) {
        this.video = video;
    }

    // displaymanager

    public String getDisplaymanager() {
        return displaymanager;
    }

    public void setDisplaymanager(String displaymanager) {
        this.displaymanager = displaymanager;
    }

    // displaymanagerver

    public String getDisplaymanagerver() {
        return displaymanagerver;
    }

    public void setDisplaymanagerver(String displaymanagerver) {
        this.displaymanagerver = displaymanagerver;
    }

    // instl

    public Bool getInstl() {
        return instl == null ? Bool.FALSE : instl;
    }

    public void setInstl(Bool instl) {
        this.instl = instl;
    }

    // tagid

    public String getTagid() {
        return tagid;
    }

    public void setTagid(String tagid) {
        this.tagid = tagid;
    }

    // bidfloor

    public Float getBidfloor() {
        return bidfloor;
    }

    public void setBidfloor(Float bidfloor) {
        this.bidfloor = bidfloor;
    }

    // bidfloorcur

    public String getBidfloorcur() {
        return bidfloorcur;
    }

    public void setBidfloorcur(String bidfloorcur) {
        this.bidfloorcur = bidfloorcur;
    }

    // iframebuster

    public List<String> getIframebusterList() {
        return iframebuster;
    }

    public void setIframebusterList(List<String> iframebuster) {
        this.iframebuster = iframebuster;
    }

    // ext

    public ImpressionExt getExt() {
        return ext;
    }

    public void setExt(ImpressionExt ext) {
        this.ext = ext;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<Impression> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<Impression> SCHEMA = new Schema<Impression>() {
        // schema methods

        public Impression newMessage() {
            return new Impression();
        }

        public Class<Impression> typeClass() {
            return Impression.class;
        }

        public String messageName() {
            return Impression.class.getSimpleName();
        }

        public String messageFullName() {
            return Impression.class.getName();
        }

        public boolean isInitialized(Impression message) {
            return
                    message.id != null;
        }

        public void mergeFrom(Input input, Impression message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.id = input.readString();
                        break;
                    case 2:
                        message.banner = input.mergeObject(message.banner, Banner.getSchema());
                        break;

                    case 3:
                        message.video = input.mergeObject(message.video, Video.getSchema());
                        break;

                    case 4:
                        message.displaymanager = input.readString();
                        break;
                    case 5:
                        message.displaymanagerver = input.readString();
                        break;
                    case 6:
                        message.instl = Bool.valueOf(input.readEnum());
                        break;
                    case 7:
                        message.tagid = input.readString();
                        break;
                    case 8:
                        message.bidfloor = input.readFloat();
                        break;
                    case 9:
                        message.bidfloorcur = input.readString();
                        break;
                    case 10:
                        if (message.iframebuster == null)
                            message.iframebuster = new ArrayList<String>();
                        message.iframebuster.add(input.readString());
                        break;
                    case 11:
                        message.ext = input.mergeObject(message.ext, ImpressionExt.getSchema());
                        break;

                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, Impression message) throws IOException {
            if (message.id == null)
                throw new UninitializedMessageException(message);
            output.writeString(1, message.id, false);

            if (message.banner != null)
                output.writeObject(2, message.banner, Banner.getSchema(), false);


            if (message.video != null)
                output.writeObject(3, message.video, Video.getSchema(), false);


            if (message.displaymanager != null)
                output.writeString(4, message.displaymanager, false);

            if (message.displaymanagerver != null)
                output.writeString(5, message.displaymanagerver, false);

            if (message.instl != null)
                output.writeEnum(6, message.instl.number, false);

            if (message.tagid != null)
                output.writeString(7, message.tagid, false);

            if (message.bidfloor != null)
                output.writeFloat(8, message.bidfloor, false);

            if (message.bidfloorcur != null)
                output.writeString(9, message.bidfloorcur, false);

            if (message.iframebuster != null) {
                for (String iframebuster : message.iframebuster) {
                    if (iframebuster != null)
                        output.writeString(10, iframebuster, true);
                }
            }

            if (message.ext != null)
                output.writeObject(11, message.ext, ImpressionExt.getSchema(), false);

        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "id";
                case 2:
                    return "banner";
                case 3:
                    return "video";
                case 4:
                    return "displaymanager";
                case 5:
                    return "displaymanagerver";
                case 6:
                    return "instl";
                case 7:
                    return "tagid";
                case 8:
                    return "bidfloor";
                case 9:
                    return "bidfloorcur";
                case 10:
                    return "iframebuster";
                case 11:
                    return "ext";
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
            fieldMap.put("banner", 2);
            fieldMap.put("video", 3);
            fieldMap.put("displaymanager", 4);
            fieldMap.put("displaymanagerver", 5);
            fieldMap.put("instl", 6);
            fieldMap.put("tagid", 7);
            fieldMap.put("bidfloor", 8);
            fieldMap.put("bidfloorcur", 9);
            fieldMap.put("iframebuster", 10);
            fieldMap.put("ext", 11);
        }
    };

}
