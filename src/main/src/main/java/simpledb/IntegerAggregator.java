package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private TupleDesc td;

    private int totalCount = 0;
    private Integer agg = null;

    private HashMap<Field, Tuple> aggMap;
    private HashMap<Field, Integer> countMap;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        this.td = gbfield != NO_GROUPING ? new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}) : new TupleDesc(new Type[]{Type.INT_TYPE});
        aggMap = gbfield != NO_GROUPING ? new HashMap<>() : null;
        countMap = gbfield != NO_GROUPING ? new HashMap<>() : null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        totalCount += 1;
        IntField intField = (IntField) tup.getField(afield);

        if (gbfield != NO_GROUPING) {
            var field = tup.getField(gbfield);
            Tuple tuple = null;

            if (aggMap.containsKey(field)) {
                tuple = aggMap.get(field);
                IntField aggField = (IntField) tuple.getField(1);

                switch (op) {
                    case COUNT:
                        tuple.setField(1, new IntField(aggField.getValue() + 1));
                        break;
                    case MIN:
                        tuple.setField(1, new IntField(Math.min(aggField.getValue(), intField.getValue())));
                        break;
                    case MAX:
                        tuple.setField(1, new IntField(Math.max(aggField.getValue(), intField.getValue())));
                        break;
                    case SUM:
                        tuple.setField(1, new IntField(aggField.getValue() + intField.getValue()));
                        break;
                    case AVG:
                        tuple.setField(1, new IntField(aggField.getValue() + intField.getValue()));
                        break;
                }
                countMap.put(field, countMap.get(field) + 1);
            } else {
                tuple = new Tuple(td);
                tuple.setField(0, field);

                switch (op) {
                    case COUNT:
                        tuple.setField(1, new IntField(1));
                        break;
                    default:
                        tuple.setField(1, new IntField(intField.getValue()));
                }

                aggMap.put(field, tuple);
                countMap.put(field, 1);
            }
        } else {
            if (agg == null) {
                agg = intField.getValue();
            } else {
                switch (op) {
                    case SUM:
                        agg += intField.getValue();
                        break;
                    case AVG:
                        agg += intField.getValue();
                        break;
                    case MIN:
                        agg = Math.min(agg, intField.getValue());
                        break;
                    case MAX:
                        agg = Math.max(agg, intField.getValue());
                        break;
                }
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for proj2");
        if (gbfield == NO_GROUPING) {
            IntField field = null;
            switch (op) {
                case COUNT:
                    field = new IntField(totalCount);
                    break;
                case AVG:
                    field = new IntField(agg / totalCount);
                    break;
                default:
                    field = new IntField(agg);
            }

            var tuple = new Tuple(td);
            tuple.setField(0, field);
            List<Tuple> tupleList = new ArrayList<>();
            tupleList.add(tuple);

            return new TupleIterator(td, tupleList);
        }

        List<Tuple> tupleList = new ArrayList<>();
        for (var entry: aggMap.entrySet()) {
            var tuple = entry.getValue();
            if (op == Op.AVG) {
                IntField aggField = (IntField) tuple.getField(1);
                var newTuple = new Tuple(td);
                newTuple.setField(0, tuple.getField(0));
                newTuple.setField(1, new IntField(aggField.getValue() / countMap.get(entry.getKey())));

                tupleList.add(newTuple);
            } else {
                tupleList.add(tuple);
            }
        }

        return new TupleIterator(td, tupleList);
    }

}
