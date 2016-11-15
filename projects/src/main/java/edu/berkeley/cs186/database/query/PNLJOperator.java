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
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
      int rightNumPages = getRightSource().getStats().getNumPages();
      int leftNumPages = getLeftSource().getStats().getNumPages();
      return (leftNumPages * rightNumPages) + leftNumPages;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
    private class PNLJIterator implements Iterator<Record> {
      private String leftTableName;
      private String rightTableName;
      private Iterator<Page> leftIterator;
      private Iterator<Page> rightIterator;
      private Record leftRecord;
      private Record nextRecord;
      private Record rightRecord;
      private Page leftPage;
      private Page rightPage;

      private Iterator<Record> leftRecordIteratorForPage;
      private Iterator<Record> rightRecordIteratorForPage;

      public PNLJIterator() throws QueryPlanException, DatabaseException {
          if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
              this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
          } else {
              this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
              Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
              while (leftIter.hasNext()) {
                  PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
              }
          }

          if (PNLJOperator.this.getRightSource().isSequentialScan()) {
              this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
          } else {
              this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
              Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
              while (rightIter.hasNext()) {
                  PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
              }
          }

          this.leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
          this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
          this.leftRecord = null;
          this.nextRecord = null;
          this.rightRecord = null;
          this.leftPage = null;
          this.rightPage = null;

          if (leftIterator.hasNext()) {
              this.leftPage = leftIterator.next();
              if (leftPage.getPageNum() == 0) {
                  this.leftPage = leftIterator.next();
              }
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
              this.pageHeader = PNLJOperator.this.getPageHeader(tableName, page);
              this.entryNum = 0;
              this.nextRecord = null;
              this.isLeft = isLeft;
              this.numEntriesPerPage = PNLJOperator.this.getNumEntriesPerPage(tableName);
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
                          int entrySize = PNLJOperator.this.getEntrySize(tableName);
                          int offset = PNLJOperator.this.getHeaderSize(tableName) + (entrySize * entryNum);
                          byte[] bytes = page.readBytes(offset, entrySize);

                          if (this.isLeft) {
                              this.nextRecord = PNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
                          } else {
                              this.nextRecord = PNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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

    public boolean hasNext() {
        if (this.nextRecord != null) {
            return true;
        }
        try {
            while (true) {
                while (this.leftRecord == null) {
                    if (leftRecordIteratorForPage.hasNext()) {
                        this.leftRecord = leftRecordIteratorForPage.next();
                        this.rightRecordIteratorForPage = new RecordIteratorForPage(rightTableName, rightPage, false);
                    } else {
                        if (rightIterator.hasNext()) {
                            this.rightPage = rightIterator.next();
                            this.leftRecordIteratorForPage = new RecordIteratorForPage(leftTableName, leftPage, true);
                        } else {
                            if (leftIterator.hasNext()) {
                                this.leftPage = leftIterator.next();
                                this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
                            } else {
                                return false;
                            }
                        }
                    }
                }
                while (this.rightRecordIteratorForPage.hasNext()) {
                    this.rightRecord = rightRecordIteratorForPage.next();

                    DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    }
                }
                this.leftRecord = null;
            }
        } catch (DatabaseException e) {
            return false;
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
