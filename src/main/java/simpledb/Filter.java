package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate predicate;
    private DbIterator child;
    private Tuple nextTuple;

    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
        this.nextTuple = null;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
    }

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        nextTuple = null;
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (child.hasNext()) {
            var target = child.next();
            if (predicate.filter(target)) {
                return target;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        // some code goes here
        nextTuple = fetchNext();
        return nextTuple != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        var ans = nextTuple;
        if (nextTuple != null) {
            nextTuple = null;
            return ans;
        }

        while (true) {
            var target = child.next();
            if (predicate.filter(target)) {
                return target;
            }
        }
    }
}
