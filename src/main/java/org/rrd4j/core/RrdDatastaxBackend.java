package org.rrd4j.core;

import com.datastax.driver.mapping.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>RrdDatastaxBackend class.</p>
 *
 * @author Kasper Fock
 */
public class RrdDatastaxBackend extends RrdByteArrayBackend {
    private final Mapper<RrdDatastax> mapper;

    /**
     * <p>Constructor for RrdDatastaxBackend.</p>
     *
     * @param path   a {@link String} object.
     * @param mapper datastax mapper for RrdDatastax
     */
    public RrdDatastaxBackend(String path, Mapper<RrdDatastax> mapper) {
        super(path);
        this.mapper = mapper;
        RrdDatastax rrdObject = mapper.get(path);
        if (rrdObject != null) {
            setBuffer(rrdObject.getRrd().array());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (isDirty()) {
            try {
                mapper.save(new RrdDatastax().setPath(getPath()).setRrd(ByteBuffer.wrap(getBuffer())));
            } catch (Throwable t) {
                throw new IOException("Failed to store", t);
            }
        }
    }

}
