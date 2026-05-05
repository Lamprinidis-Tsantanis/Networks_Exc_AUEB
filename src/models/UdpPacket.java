package models;

import java.io.Serializable;

public class UdpPacket implements Serializable {
    private final int seqId;
    private final byte[] payload;
    private final Boolean isAck;     // is this package an Ack package?
    private final Boolean isFinal;   // Is this package the final package of the transaction

    public UdpPacket(int seqId, byte[] payload, Boolean isAck) {
        this.seqId = seqId;
        this.payload = payload;
        this.isAck = isAck;
        this.isFinal = false;
    }
    public UdpPacket(int seqId, byte[] payload, Boolean isAck, Boolean isFinal) {
        this.seqId = seqId;
        this.payload = payload;
        this.isAck = isAck;
        this.isFinal = isFinal;
    }

    static public UdpPacket createAck(int seqId){
        return new UdpPacket(seqId,new byte[0],true);
    }

    public int getSeqId() {return seqId;}
    public byte[] getPayload() {return payload;}
    public Boolean getIsAck() {return isAck;}
    public Boolean getIsFinal() {return isFinal;}

}
