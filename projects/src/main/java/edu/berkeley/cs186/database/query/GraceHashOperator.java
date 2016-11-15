package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    int leftNumPages = getLeftSource().getStats().getNumPages();
    int rightNumPages = getRightSource().getStats().getNumPages();
    return 3 * (leftNumPages + rightNumPages);
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private Record rightRecord;
    private Record nextRecord;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private int currentPartition;
    private Map<DataType, ArrayList<Record>> inMemoryHashTable;

    private Record leftRecord;
    private Iterator<Record> matchingRecordIterator;
    private ArrayList<Record> matchingRecords;
    private Iterator<Record> rIter;
    private Iterator<Record> lIter;

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }

      int leftColumnIndex = GraceHashOperator.this.getLeftColumnIndex();
      int rightColumnIndex = GraceHashOperator.this.getRightColumnIndex();
      String tableName;

      while (leftIterator.hasNext()) {
        leftRecord = leftIterator.next();
        DataType leftColumn = leftRecord.getValues().get(leftColumnIndex);
        int leftHashCode = leftColumn.hashCode();
        int leftIndex = leftHashCode % leftPartitions.length;
        tableName = leftPartitions[leftIndex];

        GraceHashOperator.this.addRecord(tableName, leftRecord.getValues());
      }

      while (rightIterator.hasNext()) {
        rightRecord = rightIterator.next();
        DataType rightColumn = rightRecord.getValues().get(rightColumnIndex);
        int rightHashCode = rightColumn.hashCode();
        int rightIndex = rightHashCode % rightPartitions.length;
        tableName = rightPartitions[rightIndex];

        GraceHashOperator.this.addRecord(tableName, rightRecord.getValues());
      }

      inMemoryHashTable = new HashMap<DataType, ArrayList<Record>>();
      matchingRecords = new ArrayList<Record>();
      matchingRecordIterator = null;

      currentPartition = 0;
      rightTableName = rightPartitions[currentPartition];
      leftTableName = leftPartitions[currentPartition];
      this.lIter = getTableIterator(leftTableName);
      this.rIter = getTableIterator(rightTableName);
      while ((!lIter.hasNext()) && (!rIter.hasNext())) {
        currentPartition += 1;
        rightTableName = rightPartitions[currentPartition];
        leftTableName = leftPartitions[currentPartition];
        this.lIter = getTableIterator(leftTableName);
        this.rIter = getTableIterator(rightTableName);
      }

      while (lIter.hasNext()) {
        leftRecord = lIter.next();
        DataType leftColumn = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());

        if (!inMemoryHashTable.containsKey(leftColumn)) {
          ArrayList<Record> records = new ArrayList<Record>();
          records.add(leftRecord);
          inMemoryHashTable.put(leftColumn, records);
        } else {
          ArrayList<Record> records = inMemoryHashTable.get(leftColumn);
          records.add(leftRecord);
          inMemoryHashTable.put(leftColumn, records);
        }
      }
      if (rIter.hasNext()) {
        rightRecord = rIter.next();
        DataType rightColumn = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());

        if (inMemoryHashTable.containsKey(rightColumn)) {
          ArrayList<Record> leftRecords = inMemoryHashTable.get(rightColumn);
          for (int i = 0; i < leftRecords.size(); i++) {
            leftRecord = leftRecords.get(i);

            List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
            List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            Record matchingRecord = new Record(leftValues);
            matchingRecords.add(matchingRecord);
          }
        }
        matchingRecordIterator = matchingRecords.iterator();
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (matchingRecordIterator.hasNext()) {
        return true;
      } else {
        try {
          matchingRecords = new ArrayList<Record>();
          while (rIter.hasNext()) {
            rightRecord = rIter.next();
            DataType rightColumn = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            if (inMemoryHashTable.containsKey(rightColumn)) {
              ArrayList<Record> leftRecords = inMemoryHashTable.get(rightColumn);
              for (int i = 0; i < leftRecords.size(); i++) {
                leftRecord = leftRecords.get(i);

                List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
                List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                leftValues.addAll(rightValues);
                Record matchingRecord = new Record(leftValues);
                matchingRecords.add(matchingRecord);
              }
            }
            matchingRecordIterator = matchingRecords.iterator();
            if (this.matchingRecordIterator.hasNext()) {
              return true;
            }
          }
          currentPartition += 1;
          while (currentPartition < rightPartitions.length) {
            String rightTableName = rightPartitions[currentPartition];
            String leftTableName = leftPartitions[currentPartition];
            this.lIter = getTableIterator(leftTableName);
            this.rIter = getTableIterator(rightTableName);

            while (lIter.hasNext()) {
              leftRecord = lIter.next();
              DataType leftColumn = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());

              if (!inMemoryHashTable.containsKey(leftColumn)) {
                ArrayList<Record> records = new ArrayList<Record>();
                records.add(leftRecord);
                inMemoryHashTable.put(leftColumn, records);
              } else {
                ArrayList<Record> records = inMemoryHashTable.get(leftColumn);
                records.add(leftRecord);
                inMemoryHashTable.put(leftColumn, records);
              }
            }

            if (rIter.hasNext()) {
              rightRecord = rIter.next();
              DataType rightColumn = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
              if (inMemoryHashTable.containsKey(rightColumn)) {
                ArrayList<Record> leftRecords = inMemoryHashTable.get(rightColumn);
                for (int i = 0; i < leftRecords.size(); i++) {
                  leftRecord = leftRecords.get(i);

                  List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
                  List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                  leftValues.addAll(rightValues);
                  Record matchingRecord = new Record(leftValues);
                  matchingRecords.add(matchingRecord);
                }
              }
            }
            this.matchingRecordIterator = matchingRecords.iterator();
            if (this.matchingRecordIterator.hasNext()) {
              return true;
            }
            currentPartition += 1;
          }
          return false;
        } catch (DatabaseException e) {
          return false;
        }
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        return matchingRecordIterator.next();
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
