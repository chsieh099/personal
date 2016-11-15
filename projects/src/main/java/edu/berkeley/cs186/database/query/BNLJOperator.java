package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    int leftNumPages = getLeftSource().getStats().getNumPages();
    int rightNumPages = getRightSource().getStats().getNumPages();
    return (int) (Math.ceil((double)leftNumPages / (numBuffers - 2)) * rightNumPages) + leftNumPages;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private Page[] block;

    private Iterator<Record> leftRecordIteratorForPage;
    private Iterator<Record> rightRecordIteratorForPage;
    private int blockIndex;
    private int blockFilled; /* Index one AFTER last element of block */

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }

      this.leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
      this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
      this.leftRecord = null;
      this.nextRecord = null;
      this.rightRecord = null;
      this.leftPage = null;
      this.rightPage = null;
      this.block = new Page[numBuffers - 2];

      if (leftIterator.hasNext()) {
        this.leftPage = leftIterator.next();
        this.block[0] = leftPage;
        if (leftPage.getPageNum() == 0) {
          this.leftPage = leftIterator.next();
          this.block[0] = leftPage;
        }
        int counter = 1;
        while ((counter < this.block.length) && (leftIterator.hasNext())) {
          this.block[counter] = leftIterator.next();
          counter += 1;
        }
        this.blockFilled = counter;
        this.blockIndex = 0;

        this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
      }

      if (rightIterator.hasNext()) {
        this.rightPage = rightIterator.next();
        if (rightPage.getPageNum() == 0) {
          this.rightPage = rightIterator.next();
        }
        this.rightRecordIteratorForPage = new RecordIteratorForPage(rightTableName, rightPage, false);
      }
    }

    /**
     * Given a tablename, page, and whether the page is from the left source or right
     * source, creates an iterator for the records in a page.
     */
    private class RecordIteratorForPage implements Iterator<Record>{
      private String tableName;
      private Page page;
      private byte[] pageHeader;
      private int entryNum;
      private Record nextRecord;
      private boolean isLeft;
      private int numEntriesPerPage;

      public RecordIteratorForPage(String tableName, Page page, boolean isLeft) throws DatabaseException {
        this.tableName = tableName;
        this.page = page;
        this.pageHeader = BNLJOperator.this.getPageHeader(tableName, page);
        this.entryNum = 0;
        this.nextRecord = null;
        this.isLeft = isLeft;
        this.numEntriesPerPage = BNLJOperator.this.getNumEntriesPerPage(tableName);
      }

      /**
       * Checks if next record exists on page.
       * @return true if this iterator has another record to yield, otherwise false.
       *
       */
      public boolean hasNext() {
        if (this.nextRecord != null) {
          return true;
        }
        while (this.entryNum < this.numEntriesPerPage) {
          byte b = pageHeader[this.entryNum/8];
          int bitOffset = 7 - (this.entryNum % 8);
          byte mask = (byte) (1 << bitOffset);

          byte value = (byte) (b & mask);

          if (value != 0) {
            try {
              int entrySize = BNLJOperator.this.getEntrySize(tableName);
              int offset = BNLJOperator.this.getHeaderSize(tableName) + (entrySize * entryNum);
              byte[] bytes = page.readBytes(offset, entrySize);

              if (this.isLeft) {
                this.nextRecord = BNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
              } else {
                this.nextRecord = BNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
              }
              return true;
            } catch (DatabaseException e) {
              return false;
            }
          }
          this.entryNum += 1;
        }

        return false;
      }

      /**
       * Gets next record on page.
       * @return the next record
       * @throws NoSuchElementException if there are no more Records to yield
       */
      public Record next() {
        if (hasNext()) {
          Record recordToReturn = this.nextRecord;
          this.nextRecord = null;
          this.entryNum += 1;
          return recordToReturn;
        }
        throw new NoSuchElementException();
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      try {
        while (true) {
          while (this.leftRecord == null) {
            if (leftRecordIteratorForPage.hasNext()) {
              this.leftRecord = this.leftRecordIteratorForPage.next();
              this.rightRecordIteratorForPage = new RecordIteratorForPage(rightTableName, rightPage, false);
            } else {
              if (blockIndex + 1 < blockFilled) {
                this.blockIndex += 1;
                this.leftPage = this.block[blockIndex];
                this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
              } else {
                if (rightIterator.hasNext()) {
                  this.rightPage = this.rightIterator.next();
                  this.blockIndex = 0;
                  this.leftPage = this.block[blockIndex];
                  this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
                } else {
                  if (leftIterator.hasNext()) {
                    this.blockIndex = 0;
                    this.blockFilled = 0;
                    int counter = 0;
                    while ((counter < this.block.length) && (this.leftIterator.hasNext())) {
                      this.block[counter] = leftIterator.next();
                      counter += 1;
                    }
                    this.blockFilled = counter;
                    this.leftPage = this.block[0];
                    this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
                    this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
                    if (this.rightIterator.hasNext()) {
                      this.rightPage = rightIterator.next();
                      this.rightRecordIteratorForPage = new RecordIteratorForPage(rightTableName, rightPage, false);
                    } else {
                      return false; /* not sure about this condition here. should be right because if no right pages can't go */
                    }
                  } else {
                    return false;
                  }
                }
              }
            }
          }

          while ((leftRecord != null) && (rightRecordIteratorForPage.hasNext() || rightIterator.hasNext())) {
            if (rightRecordIteratorForPage.hasNext()) {
              this.rightRecord = rightRecordIteratorForPage.next();
              DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
              DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
              if (leftJoinValue.equals(rightJoinValue)) {
                List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                leftValues.addAll(rightValues);
                this.nextRecord = new Record(leftValues);
                return true;
              }
            } else {
              this.leftRecord = null;
            }
          }

          this.leftRecord = null;
        }
      } catch (DatabaseException e) {
        return false;
      }
    }

//    /**
//     * Checks if there are more record(s) to yield
//     *
//     * @return true if this iterator has another record to yield, otherwise false
//     */
//    public boolean hasNext() {
//      if (this.nextRecord != null) {
//        return true;
//      }
//      try {
//        while (true) {
//          while (this.leftRecord == null) {
//            this.blockIndex += 1;
//            if (this.blockIndex < this.blockFilled) {
//              this.leftPage = this.block[blockIndex];
//              this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
//              if (this.leftRecordIteratorForPage.hasNext()) {
//                this.leftRecord = leftRecordIteratorForPage.next();
//              }
//            } else { /* Refill the block. */
//              this.blockFilled = 0;
//              int counter = 0;
//              this.leftPage = null;
//              while ((counter < this.block.length) && (leftIterator.hasNext())) {
//                this.block[counter] = leftIterator.next();
//                if (this.leftPage == null) {
//                  this.leftPage = this.block[counter];
//                  this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
//                  if (this.leftRecordIteratorForPage.hasNext()) {
//                    this.leftRecord = this.leftRecordIteratorForPage.next();
//                  }
//                }
//                counter += 1;
//                this.blockFilled = counter;
//              }
//              this.blockIndex = 0;
//              if (this.leftPage == null) { /* No more left pages left. */
//                return false;
//              }
//            }
//          }
//
//          while (this.rightRecordIteratorForPage.hasNext() || this.rightIterator.hasNext()) {
//            if (this.rightRecordIteratorForPage.hasNext()) {
//              this.rightRecord = rightRecordIteratorForPage.next();
//            } else { /* Went through all records in right page, reset left block iterator. */
//              this.rightRecord = null;
//
//              if (this.rightIterator.hasNext()) {
//                this.rightPage = this.rightIterator.next();
//                this.rightRecordIteratorForPage = new RecordIteratorForPage(rightTableName, rightPage, false);
//                if (this.rightRecordIteratorForPage.hasNext()) {
//                  this.rightRecord = this.rightRecordIteratorForPage.next();
//
//                  this.blockIndex = 0;
//                  this.leftPage = this.block[blockIndex];
//                  this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
//                  if (this.leftRecordIteratorForPage.hasNext()) {
//                    this.leftRecord = this.leftRecordIteratorForPage.next();
//                  }
//                }
//              }
//            }
//
//            DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
//            DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
//            if (leftJoinValue.equals(rightJoinValue)) {
//              List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
//              List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
//
//              leftValues.addAll(rightValues);
//              this.nextRecord = new Record(leftValues);
//              return true;
//            }
//          }
//
//          this.leftRecord = null;
//        }
//      } catch (DatabaseException e) {
//        return false;
//      }
//    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        Record recordToReturn = this.nextRecord;
        this.nextRecord = null;
        return recordToReturn;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
