// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from openrtb.proto

package org.openrtb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class Geo implements Externalizable, Message<Geo> {

    public static Schema<Geo> getSchema() {
        return SCHEMA;
    }

    public static Geo getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final Geo DEFAULT_INSTANCE = new Geo();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    Float lat;
    Float lon;
    String country;
    String region;
    String regionfips104;
    String metro;
    String city;
    String zip;
    Integer type;

    public Geo() {

    }

    // getters and setters

    // lat

    public Float getLat() {
        return lat;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    // lon

    public Float getLon() {
        return lon;
    }

    public void setLon(Float lon) {
        this.lon = lon;
    }

    // country

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    // region

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    // regionfips104

    public String getRegionfips104() {
        return regionfips104;
    }

    public void setRegionfips104(String regionfips104) {
        this.regionfips104 = regionfips104;
    }

    // metro

    public String getMetro() {
        return metro;
    }

    public void setMetro(String metro) {
        this.metro = metro;
    }

    // city

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    // zip

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    // type

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<Geo> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<Geo> SCHEMA = new Schema<Geo>() {
        // schema methods

        public Geo newMessage() {
            return new Geo();
        }

        public Class<Geo> typeClass() {
            return Geo.class;
        }

        public String messageName() {
            return Geo.class.getSimpleName();
        }

        public String messageFullName() {
            return Geo.class.getName();
        }

        public boolean isInitialized(Geo message) {
            return true;
        }

        public void mergeFrom(Input input, Geo message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.lat = input.readFloat();
                        break;
                    case 2:
                        message.lon = input.readFloat();
                        break;
                    case 3:
                        message.country = input.readString();
                        break;
                    case 4:
                        message.region = input.readString();
                        break;
                    case 5:
                        message.regionfips104 = input.readString();
                        break;
                    case 6:
                        message.metro = input.readString();
                        break;
                    case 7:
                        message.city = input.readString();
                        break;
                    case 8:
                        message.zip = input.readString();
                        break;
                    case 9:
                        message.type = input.readInt32();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, Geo message) throws IOException {
            if (message.lat != null)
                output.writeFloat(1, message.lat, false);

            if (message.lon != null)
                output.writeFloat(2, message.lon, false);

            if (message.country != null)
                output.writeString(3, message.country, false);

            if (message.region != null)
                output.writeString(4, message.region, false);

            if (message.regionfips104 != null)
                output.writeString(5, message.regionfips104, false);

            if (message.metro != null)
                output.writeString(6, message.metro, false);

            if (message.city != null)
                output.writeString(7, message.city, false);

            if (message.zip != null)
                output.writeString(8, message.zip, false);

            if (message.type != null)
                output.writeInt32(9, message.type, false);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "lat";
                case 2:
                    return "lon";
                case 3:
                    return "country";
                case 4:
                    return "region";
                case 5:
                    return "regionfips104";
                case 6:
                    return "metro";
                case 7:
                    return "city";
                case 8:
                    return "zip";
                case 9:
                    return "type";
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
            fieldMap.put("lat", 1);
            fieldMap.put("lon", 2);
            fieldMap.put("country", 3);
            fieldMap.put("region", 4);
            fieldMap.put("regionfips104", 5);
            fieldMap.put("metro", 6);
            fieldMap.put("city", 7);
            fieldMap.put("zip", 8);
            fieldMap.put("type", 9);
        }
    };

}
