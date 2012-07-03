package com.sonar.expedition.scrawler.objs;

public class CheckinObjects {
    String serviceType = "";
    String serviceProfileId = "";
    String serviceCheckinId = "";
    String venueName = "";
    String venueAddress = "";
    String checkinTime;
    String geohash = "";
    String latitude = "";
    String longitude = "";
    String message = "";

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getServiceProfileId() {
        return serviceProfileId;
    }

    public void setServiceProfileId(String serviceProfileId) {
        this.serviceProfileId = serviceProfileId;
    }

    public String getServiceCheckinId() {
        return serviceCheckinId;
    }

    public void setServiceCheckinId(String serviceCheckinId) {
        this.serviceCheckinId = serviceCheckinId;
    }

    public String getVenueName() {
        return venueName;
    }

    public String getLocation() {
        return latitude + "," + longitude;
    }

    public void setVenueName(String venueName) {
        this.venueName = venueName;
    }

    public String getVenueAddress() {
        return venueAddress;
    }

    public void setVenueAddress(String venueAddress) {
        this.venueAddress = venueAddress;
    }

    public String getCheckinTime() {
        return checkinTime;
    }

    public void setCheckinTime(String checkinTime) {
        this.checkinTime = checkinTime;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
