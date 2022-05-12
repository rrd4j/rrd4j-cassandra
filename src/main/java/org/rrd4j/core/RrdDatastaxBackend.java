package org.rrd4j.core;



import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>RrdDatastaxBackend class.</p>
 *
 * @author Kasper Fock
 */
public class RrdDatastaxBackend extends RrdByteArrayBackend {
//    private final Mapper<RrdDatastax> mapper;
    private RrdDatastax data;
    private RrdDatastaxDao dao;
    private boolean readonly;
    private boolean exists;

    /**
     * <p>Constructor for RrdDatastaxBackend.</p>
     *
     * @param path   a {@link String} object.
     */
    public RrdDatastaxBackend(String path,RrdDatastaxDao dao,boolean readonly) {
        super(path);
        this.readonly = readonly;
        this.dao = dao;
        this.data = dao.findByPath(path);
        this.exists = this.data != null;
        if(!this.exists){
            this.data = new RrdDatastax().setPath(path).setRrd(ByteBuffer.allocate(0));
        }
        this.setByteBuffer(this.data.getRrd());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (isDirty() && !this.readonly) {
            try {
                this.data.setRrd(ByteBuffer.wrap(getBuffer()));
                if(this.exists){
                    this.dao.update(this.data);
                }else{
                    this.dao.create(this.data);
                }
            } catch (Throwable t) {
                throw new IOException("Failed to store", t);
            }
        }
    }

}
