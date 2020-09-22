package com.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;

import java.io.IOException;

public class DeSerialization implements DeserializationSchema<TransPacket>{
    private static ParsedPacketDecoder decoder = new ParsedPacketDecoder();

    @Override
    public TransPacket deserialize(byte[] bytes) throws IOException {
        try {
            TransPacket transPacket = decoder.deserialize(null,bytes);
            return transPacket;
        } catch (Exception e) {
            e.printStackTrace();
            TransPacket packet = new TransPacket();
            packet.setRawDataId("sk7878");
            return packet;
        }
    }

    @Override
    public boolean isEndOfStream(TransPacket transPacket) {
        return false;
    }

    @Override
    public TypeInformation<TransPacket> getProducedType() {
        return TypeInformation.of(TransPacket.class);
    }
}
