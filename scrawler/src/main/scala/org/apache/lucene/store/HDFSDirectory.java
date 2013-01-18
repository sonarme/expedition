package org.apache.lucene.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * An HDFS based Lucene Directory implementation.
 * <p/>
 * MMap'ing the block file is used for efficiency, however it's recommended only
 * for 64-bit Unix OS systems
 */
public class HDFSDirectory extends Directory {
    private static final Log LOG = LogFactory.getLog(HDFSDirectory.class);
    private FileSystem fileSystem;
    private String rootPath;
    protected int bufferSize;

    public HDFSDirectory(FileSystem fileSystem, String path) throws IOException {
        this.fileSystem = fileSystem;
        this.rootPath = path;
        setLockFactory(new HDFSLockFactory(path, fileSystem));
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return new HDFSIndexOutput(getPath(name));
    }

    protected class HDFSIndexOutput extends BufferedIndexOutput {
        private Path path;
        private FSDataOutputStream output;

        public HDFSIndexOutput(Path path) throws IOException {
            this.path = path;
            short replication = fileSystem.getDefaultReplication();
            Configuration conf = fileSystem.getConf();
            bufferSize = conf.getInt("io.file.buffer.size", 4096);
            long blockSize = (long) 32 * 1024 * 1024 * 1024;
            output = fileSystem.create(path, false, bufferSize, replication,
                    blockSize);
        }

        @Override
        protected void flushBuffer(byte[] b, int offset, int len)
                throws IOException {
            output.write(b, offset, len);
        }

        @Override
        public void flush() throws IOException {
            super.flush();
            output.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
            output.close();
        }

        @Override
        public void seek(long pos) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() throws IOException {
            return output.getPos();
        }

        @Override
        public void setLength(long length) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public String[] listAll() throws IOException {
        List<String> files = new ArrayList<String>();
        FileStatus[] statuses = fileSystem.listStatus(new Path(rootPath));
        for (FileStatus status : statuses) {
            files.add(status.getPath().getName());
        }
        return (String[]) files.toArray(new String[0]);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fileSystem.getFileStatus(new Path(rootPath, name)).getLen();
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileSystem.exists(new Path(rootPath, name));
    }

    @Override
    public long fileModified(String name) throws IOException {
        return fileSystem.getFileStatus(new Path(rootPath, name))
                .getModificationTime();
    }

    @Override
    public void touchFile(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void deleteFile(String name) throws IOException {
        Path path = getPath(name);
        boolean deleted = fileSystem.delete(path, false);
    }

    private Path getPath(String name) {
        return new Path(rootPath + "/" + name);
    }




    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {



        Path path = getPath(name);
            // open the HDFS input to obtain the
            // underlying block file
            final FSDataInputStream dataInput = fileSystem.open(path, bufferSize);

            final DFSClient.DFSInputStream dfsInput = (DFSClient.DFSInputStream) dataInput.getInput();
            try {
              File file = dfsInput.getFile();
              if (file == null) {
                throw new IOException("file is null");
              }
              RandomAccessFile raf = new RandomAccessFile(file, "r");
              try {
                return new HDFSMMapIndexInput(raf);
              } finally {
                raf.close();
              }
            } finally {
              dfsInput.close();
            }
        return null;
    }

    private class HDFSMMapIndexInput extends IndexInput {
        private ByteBuffer buffer;
        private final long length;
        private boolean isClone = false;




        protected HDFSMMapIndexInput(String raf) throws IOException {
            super(raf);
            RandomAccessFile randomAccessFile = new RandomAccessFile(raf, "rw");
            this.length = randomAccessFile.length();
            this.buffer = randomAccessFile.getChannel().map(MapMode.READ_ONLY, 0, length);
        }

        @Override
        public byte readByte() throws IOException {
            try {
                return buffer.get();
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            try {
                buffer.get(b, offset, len);
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public short readShort() throws IOException {
            try {
                return buffer.getShort();
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public int readInt() throws IOException {
            try {
                return buffer.getInt();
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public long readLong() throws IOException {
            try {
                return buffer.getLong();
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public long getFilePointer() {
            return buffer.position();
        }

        @Override
        public void seek(long pos) throws IOException {
            buffer.position((int) pos);
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Object clone() {
            if (buffer == null)
                throw new AlreadyClosedException("MMapIndexInput already closed");
            HDFSMMapIndexInput clone = (HDFSMMapIndexInput) super.clone();
            clone.isClone = true;
            clone.buffer = buffer.duplicate();
            return clone;
        }

        @Override
        public void close() throws IOException {
            // unmap the buffer (if enabled) and at least unset it for GC
            try {
                if (isClone || buffer == null)
                    return;
                cleanMapping(buffer);
            } finally {
                buffer = null;
            }
        }
    }

    final void cleanMapping(final ByteBuffer buffer) throws IOException {
        if (MMapDirectory.UNMAP_SUPPORTED) {
            try {
                AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        final Method getCleanerMethod = buffer.getClass().getMethod(
                                "cleaner");
                        getCleanerMethod.setAccessible(true);
                        final Object cleaner = getCleanerMethod.invoke(buffer);
                        if (cleaner != null) {
                            cleaner.getClass().getMethod("clean").invoke(cleaner);
                        }
                        return null;
                    }
                });
            } catch (PrivilegedActionException e) {
                final IOException ioe = new IOException(
                        "unable to unmap the mapped buffer");
                ioe.initCause(e.getCause());
                throw ioe;
            }
        }
    }
}