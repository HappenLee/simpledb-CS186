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
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    private void findNextTuple() throws DbException, TransactionAbortedException {
        nextTuple = null;
        while (child.hasNext()) {
            var tuple = child.next();
            if (predicate.filter(tuple)) {
                nextTuple = tuple;
            }
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        findNextTuple();
    }

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        var ans = nextTuple;
        findNextTuple();
        return ans;
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
        return null != nextTuple;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return fetchNext();
    }
}
