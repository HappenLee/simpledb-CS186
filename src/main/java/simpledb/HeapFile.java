package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;;
    private TupleDesc td;
    private RandomAccessFile tableAccessFile;
    private int numPages;
    private int tupleCountPage;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        try {
            this.tableAccessFile = new RandomAccessFile(f, "rw");
            this.numPages = (int) tableAccessFile.length() / BufferPool.PAGE_SIZE;
        }catch (IOException e) {

        }
        this.tupleCountPage = BufferPool.PAGE_SIZE * 8 / (td.getSize() * 8 + 1);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid instanceof HeapPageId && pid.getTableId() == getId()) {
            HeapPageId heapPageId = (HeapPageId)(pid);
            int pageOffset = heapPageId.pageNumber() * BufferPool.PAGE_SIZE;
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            try {
                tableAccessFile.seek(pageOffset);
                tableAccessFile.read(data);
                return new HeapPage(heapPageId, data);
            } catch (IOException e) {
                Debug.log(f.getAbsolutePath() + " offset is invalid:" + pageOffset);
            }
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class TableIterator implements DbFileIterator {
            private int pageCount;
            private TransactionId tid;
            private Iterator<Tuple> tupleInterator;
            private BufferPool bufferPool = Database.getBufferPool();
            private int tableId;

            private final Iterator<Tuple> emptyIterator = new Iterator<Tuple>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Tuple next() {
                    return null;
                }
            };

            TableIterator(TransactionId tid, int tableId) {
                this.tid = tid;
                this.tableId = tableId;
                this.tupleInterator = emptyIterator;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageCount = 0;
                if (pageCount < numPages) {
                    HeapPage hp = (HeapPage)(bufferPool.getPage(tid, new HeapPageId(tableId, pageCount), Permissions.READ_ONLY));
                    tupleInterator = hp.iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                while (!tupleInterator.hasNext()) {
                        pageCount++;
                        if (pageCount >= numPages) {
                            return false;
                        }

                        HeapPage hp = (HeapPage)(bufferPool.getPage(tid, new HeapPageId(tableId, pageCount), Permissions.READ_ONLY));
                        tupleInterator = hp.iterator();
                }
                return tupleInterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                var tuple = tupleInterator.next();
                if (tuple == null){
                    throw new NoSuchElementException();
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pageCount = numPages;
                tupleInterator = emptyIterator;
            }
        }

        TableIterator tableIterator = new TableIterator(tid, getId());
        return tableIterator;
    }
}

