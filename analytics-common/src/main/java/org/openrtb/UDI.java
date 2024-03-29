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

public final class UDI implements Externalizable, Message<UDI>
{

    public static Schema<UDI> getSchema()
    {
        return SCHEMA;
    }

    public static UDI getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final UDI DEFAULT_INSTANCE = new UDI();

    
    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String androidid;
    String androididmd5;
    String androididsha1;
    String imei;
    String imeimd5;
    String imeisha1;
    String udidmd5;
    String udidsha1;
    String macmd5;
    String macsha1;
    String odin;
    String openudid;
    String idfa;
    Integer idfatracking;

    public UDI()
    {
        
    }

    // getters and setters

    // androidid

    public String getAndroidid()
    {
        return androidid;
    }

    public void setAndroidid(String androidid)
    {
        this.androidid = androidid;
    }

    // androididmd5

    public String getAndroididmd5()
    {
        return androididmd5;
    }

    public void setAndroididmd5(String androididmd5)
    {
        this.androididmd5 = androididmd5;
    }

    // androididsha1

    public String getAndroididsha1()
    {
        return androididsha1;
    }

    public void setAndroididsha1(String androididsha1)
    {
        this.androididsha1 = androididsha1;
    }

    // imei

    public String getImei()
    {
        return imei;
    }

    public void setImei(String imei)
    {
        this.imei = imei;
    }

    // imeimd5

    public String getImeimd5()
    {
        return imeimd5;
    }

    public void setImeimd5(String imeimd5)
    {
        this.imeimd5 = imeimd5;
    }

    // imeisha1

    public String getImeisha1()
    {
        return imeisha1;
    }

    public void setImeisha1(String imeisha1)
    {
        this.imeisha1 = imeisha1;
    }

    // udidmd5

    public String getUdidmd5()
    {
        return udidmd5;
    }

    public void setUdidmd5(String udidmd5)
    {
        this.udidmd5 = udidmd5;
    }

    // udidsha1

    public String getUdidsha1()
    {
        return udidsha1;
    }

    public void setUdidsha1(String udidsha1)
    {
        this.udidsha1 = udidsha1;
    }

    // macmd5

    public String getMacmd5()
    {
        return macmd5;
    }

    public void setMacmd5(String macmd5)
    {
        this.macmd5 = macmd5;
    }

    // macsha1

    public String getMacsha1()
    {
        return macsha1;
    }

    public void setMacsha1(String macsha1)
    {
        this.macsha1 = macsha1;
    }

    // odin

    public String getOdin()
    {
        return odin;
    }

    public void setOdin(String odin)
    {
        this.odin = odin;
    }

    // openudid

    public String getOpenudid()
    {
        return openudid;
    }

    public void setOpenudid(String openudid)
    {
        this.openudid = openudid;
    }

    // idfa

    public String getIdfa()
    {
        return idfa;
    }

    public void setIdfa(String idfa)
    {
        this.idfa = idfa;
    }

    // idfatracking

    public Integer getIdfatracking()
    {
        return idfatracking;
    }

    public void setIdfatracking(Integer idfatracking)
    {
        this.idfatracking = idfatracking;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<UDI> cachedSchema()
    {
        return SCHEMA;
    }

    static final Schema<UDI> SCHEMA = new Schema<UDI>()
    {
        // schema methods

        public UDI newMessage()
        {
            return new UDI();
        }

        public Class<UDI> typeClass()
        {
            return UDI.class;
        }

        public String messageName()
        {
            return UDI.class.getSimpleName();
        }

        public String messageFullName()
        {
            return UDI.class.getName();
        }

        public boolean isInitialized(UDI message)
        {
            return true;
        }

        public void mergeFrom(Input input, UDI message) throws IOException
        {
            for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
            {
                switch(number)
                {
                    case 0:
                        return;
                    case 1:
                        message.androidid = input.readString();
                        break;
                    case 2:
                        message.androididmd5 = input.readString();
                        break;
                    case 3:
                        message.androididsha1 = input.readString();
                        break;
                    case 4:
                        message.imei = input.readString();
                        break;
                    case 5:
                        message.imeimd5 = input.readString();
                        break;
                    case 6:
                        message.imeisha1 = input.readString();
                        break;
                    case 7:
                        message.udidmd5 = input.readString();
                        break;
                    case 8:
                        message.udidsha1 = input.readString();
                        break;
                    case 9:
                        message.macmd5 = input.readString();
                        break;
                    case 10:
                        message.macsha1 = input.readString();
                        break;
                    case 11:
                        message.odin = input.readString();
                        break;
                    case 12:
                        message.openudid = input.readString();
                        break;
                    case 13:
                        message.idfa = input.readString();
                        break;
                    case 14:
                        message.idfatracking = input.readInt32();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }   
            }
        }


        public void writeTo(Output output, UDI message) throws IOException
        {
            if(message.androidid != null)
                output.writeString(1, message.androidid, false);

            if(message.androididmd5 != null)
                output.writeString(2, message.androididmd5, false);

            if(message.androididsha1 != null)
                output.writeString(3, message.androididsha1, false);

            if(message.imei != null)
                output.writeString(4, message.imei, false);

            if(message.imeimd5 != null)
                output.writeString(5, message.imeimd5, false);

            if(message.imeisha1 != null)
                output.writeString(6, message.imeisha1, false);

            if(message.udidmd5 != null)
                output.writeString(7, message.udidmd5, false);

            if(message.udidsha1 != null)
                output.writeString(8, message.udidsha1, false);

            if(message.macmd5 != null)
                output.writeString(9, message.macmd5, false);

            if(message.macsha1 != null)
                output.writeString(10, message.macsha1, false);

            if(message.odin != null)
                output.writeString(11, message.odin, false);

            if(message.openudid != null)
                output.writeString(12, message.openudid, false);

            if(message.idfa != null)
                output.writeString(13, message.idfa, false);

            if(message.idfatracking != null)
                output.writeInt32(14, message.idfatracking, false);
        }

        public String getFieldName(int number)
        {
            switch(number)
            {
                case 1: return "androidid";
                case 2: return "androididmd5";
                case 3: return "androididsha1";
                case 4: return "imei";
                case 5: return "imeimd5";
                case 6: return "imeisha1";
                case 7: return "udidmd5";
                case 8: return "udidsha1";
                case 9: return "macmd5";
                case 10: return "macsha1";
                case 11: return "odin";
                case 12: return "openudid";
                case 13: return "idfa";
                case 14: return "idfatracking";
                default: return null;
            }
        }

        public int getFieldNumber(String name)
        {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("androidid", 1);
            fieldMap.put("androididmd5", 2);
            fieldMap.put("androididsha1", 3);
            fieldMap.put("imei", 4);
            fieldMap.put("imeimd5", 5);
            fieldMap.put("imeisha1", 6);
            fieldMap.put("udidmd5", 7);
            fieldMap.put("udidsha1", 8);
            fieldMap.put("macmd5", 9);
            fieldMap.put("macsha1", 10);
            fieldMap.put("odin", 11);
            fieldMap.put("openudid", 12);
            fieldMap.put("idfa", 13);
            fieldMap.put("idfatracking", 14);
        }
    };
    
}
