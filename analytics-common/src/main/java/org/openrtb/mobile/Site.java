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

public final class Site implements Externalizable, Message<Site> {

    public static Schema<Site> getSchema() {
        return SCHEMA;
    }

    public static Site getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    static final Site DEFAULT_INSTANCE = new Site();


    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String sid;
    String name;
    String domain;
    String pid;
    String pub;
    String pdomain;
    List<String> cat;
    String keywords;
    String page;
    String ref;
    String search;

    public Site() {

    }

    // getters and setters

    // sid

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    // name

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // domain

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    // pid

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    // pub

    public String getPub() {
        return pub;
    }

    public void setPub(String pub) {
        this.pub = pub;
    }

    // pdomain

    public String getPdomain() {
        return pdomain;
    }

    public void setPdomain(String pdomain) {
        this.pdomain = pdomain;
    }

    // cat

    public List<String> getCatList() {
        return cat;
    }

    public void setCatList(List<String> cat) {
        this.cat = cat;
    }

    // keywords

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    // page

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    // ref

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    // search

    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    public Schema<Site> cachedSchema() {
        return SCHEMA;
    }

    static final Schema<Site> SCHEMA = new Schema<Site>() {
        // schema methods

        public Site newMessage() {
            return new Site();
        }

        public Class<Site> typeClass() {
            return Site.class;
        }

        public String messageName() {
            return Site.class.getSimpleName();
        }

        public String messageFullName() {
            return Site.class.getName();
        }

        public boolean isInitialized(Site message) {
            return true;
        }

        public void mergeFrom(Input input, Site message) throws IOException {
            for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        message.sid = input.readString();
                        break;
                    case 2:
                        message.name = input.readString();
                        break;
                    case 3:
                        message.domain = input.readString();
                        break;
                    case 4:
                        message.pid = input.readString();
                        break;
                    case 5:
                        message.pub = input.readString();
                        break;
                    case 6:
                        message.pdomain = input.readString();
                        break;
                    case 7:
                        if (message.cat == null)
                            message.cat = new ArrayList<String>();
                        message.cat.add(input.readString());
                        break;
                    case 8:
                        message.keywords = input.readString();
                        break;
                    case 9:
                        message.page = input.readString();
                        break;
                    case 10:
                        message.ref = input.readString();
                        break;
                    case 11:
                        message.search = input.readString();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }


        public void writeTo(Output output, Site message) throws IOException {
            if (message.sid != null)
                output.writeString(1, message.sid, false);

            if (message.name != null)
                output.writeString(2, message.name, false);

            if (message.domain != null)
                output.writeString(3, message.domain, false);

            if (message.pid != null)
                output.writeString(4, message.pid, false);

            if (message.pub != null)
                output.writeString(5, message.pub, false);

            if (message.pdomain != null)
                output.writeString(6, message.pdomain, false);

            if (message.cat != null) {
                for (String cat : message.cat) {
                    if (cat != null)
                        output.writeString(7, cat, true);
                }
            }

            if (message.keywords != null)
                output.writeString(8, message.keywords, false);

            if (message.page != null)
                output.writeString(9, message.page, false);

            if (message.ref != null)
                output.writeString(10, message.ref, false);

            if (message.search != null)
                output.writeString(11, message.search, false);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "sid";
                case 2:
                    return "name";
                case 3:
                    return "domain";
                case 4:
                    return "pid";
                case 5:
                    return "pub";
                case 6:
                    return "pdomain";
                case 7:
                    return "cat";
                case 8:
                    return "keywords";
                case 9:
                    return "page";
                case 10:
                    return "ref";
                case 11:
                    return "search";
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
            fieldMap.put("sid", 1);
            fieldMap.put("name", 2);
            fieldMap.put("domain", 3);
            fieldMap.put("pid", 4);
            fieldMap.put("pub", 5);
            fieldMap.put("pdomain", 6);
            fieldMap.put("cat", 7);
            fieldMap.put("keywords", 8);
            fieldMap.put("page", 9);
            fieldMap.put("ref", 10);
            fieldMap.put("search", 11);
        }
    };

}
