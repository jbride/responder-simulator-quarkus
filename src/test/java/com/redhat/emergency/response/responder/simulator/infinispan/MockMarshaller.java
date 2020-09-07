package com.redhat.emergency.response.responder.simulator.infinispan;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

public class MockMarshaller implements Marshaller {
    @Override
    public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
        return new byte[0];
    }

    @Override
    public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
        return new byte[0];
    }

    @Override
    public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
        return null;
    }

    @Override
    public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
        return null;
    }

    @Override
    public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public boolean isMarshallable(Object o) throws Exception {
        return false;
    }

    @Override
    public BufferSizePredictor getBufferSizePredictor(Object o) {
        return null;
    }

    @Override
    public MediaType mediaType() {
        return MediaType.APPLICATION_SERIALIZED_OBJECT;
    }
}

