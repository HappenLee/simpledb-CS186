package simpledb;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static simpledb.Type.INT_TYPE;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<String, Integer> aggMap;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        aggMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        var field = tup.getField(gbfield).toString();
        if (aggMap.containsKey(field)) {
            aggMap.put(field, aggMap.get(field) + 1);
        } else {
            aggMap.put(field, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for proj2");
        List<Tuple> tupleList = null;
        TupleDesc td = null;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{INT_TYPE});
            tupleList = aggMap.values().stream().
                    map(i -> {
                        var tuple = new Tuple(new TupleDesc(new Type[]{INT_TYPE}));
                        tuple.setField(0, new IntField(i));
                        return tuple;
                    }).collect(Collectors.toList());
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, INT_TYPE});
            tupleList = aggMap.entrySet().stream().
                    map(entry -> {
                        var tuple = new Tuple(new TupleDesc(new Type[]{gbfieldtype, INT_TYPE}));
                        if (gbfieldtype.equals(INT_TYPE)) {
                            tuple.setField(0, new IntField(Integer.valueOf(entry.getKey())));
                        } else {
                            tuple.setField(0, new StringField(entry.getKey(), entry.getKey().length()));
                        }
                        tuple.setField(1, new IntField(entry.getValue()));
                        return tuple;
                    }).collect(Collectors.toList());
        }

        return new TupleIterator(td, tupleList);
    }

}
