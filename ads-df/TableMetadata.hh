/**
 * Copyright (c) 2013, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __TABLEMETATDATA_HH__
#define __TABLEMETATDATA_HH__

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "CompressionType.hh"
#include "FileSystem.hh"
#include "LogicalOperator.hh"
#include "TableFileFormat.hh"

class DynamicRecordContext;
class RecordType;

/**
 * Tables in Trecul 
 */

/**
 * A path to a file storing data for a serial organized table.
 * A path has attributes such as MinorVersion.  
 * TODO: We want path components of serial files to be metadata
 * driven (e.g. partitioning keys).  Integrate that functionality here.
 */
typedef std::shared_ptr<class SerialOrganizedTableFile> SerialOrganizedTableFilePtr;

class SerialOrganizedTableFile
{
private:
  int32_t mMinorVersion;
  std::string mDate;
  PathPtr mPath;
public:
  SerialOrganizedTableFile(int32_t minorVersion,
			   const std::string& dateField,
			   PathPtr path)
    :
    mMinorVersion(minorVersion),
    mDate(dateField),
    mPath(path)
  {
  }

  int32_t getMinorVersion() const 
  {
    return mMinorVersion;
  }
  const std::string& getDate() const 
  {
    return mDate;
  }
  PathPtr getPath() const
  {
    return mPath;
  }

  static SerialOrganizedTableFilePtr get(int32_t minorVersion, 
					 const std::string& dateField,
					 PathPtr path)
  {
    return SerialOrganizedTableFilePtr(new SerialOrganizedTableFile(minorVersion, dateField, path));
  }
};

/**
 * A table that is hash partitioned on akid 
 * and range partitioned on date.
 * This class encapsulates the directory structure that is used
 * for storing such data.
 *
 * The format for serial organized tables URIs:
 * /CommonVersion_SerialCount/TableName/DBVersionNumber/MinorNumber/Date/BatchId/FileName
 * where FileName is of the form serial_ddddd
 * 
 * To name a table requires the triple:
 * CommonVersion
 * TableName
 * DBVersionNumber
 *
 * The scanning operators can accept limiting predicates on the date.
 * The serial number is determined by the serial/Hadoop input split associated 
 * with the map job in which the operator is executing.
 *
 * We want to be able to support reading serial organized tables out of
 * a local file system as well as HDFS (or other cluster files systems in
 * the future).  Thus we use a file system abstraction beneath the path 
 * manipulations.
 *
 */
class SerialOrganizedTable
{
private:
  int32_t mCommonVersion;
  int32_t mTableMajorVersion;
  std::string mTableName;
  // Beneath the table root we have
  // a number of path components.
  // Optionally we can handle evaluating
  // predicates against these path components
  // (e.g. date ranges).
  std::vector<std::string> mPathComponents;

  // These are the paths to directories containing
  // serials that obey our predicates.
  // TODO: For our application to event_d we want the
  // date to be part of the table schema.  We should
  // probably store it here.
  std::vector<SerialOrganizedTableFilePtr> mSerialPaths;

  // Support for evaluating predicates against the
  // directory structure.
  class DynamicRecordContext * mContext;
  const class RecordType * mRecordType;
  class RecordTypeFunction * mPredicate;
  std::vector<class FieldAddress> mFields;
  std::size_t mMinorVersionField;
  std::size_t mDateField;
  class InterpreterContext * mRuntimeContext;
  
  // Recurse down directory path to get to serials.
  void bindComponent(FileSystem * fs, 
		     std::size_t level, PathPtr p);

  // Find the table root on this file system
  PathPtr getTableRoot(FileSystem * fs);
  
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mSerialPaths);
  }
  SerialOrganizedTable();
public:
  SerialOrganizedTable(int32_t commonVersion,
		       int32_t tableMajorVersion,
		       const std::string& tableName,
		       const char * pred = NULL);
  ~SerialOrganizedTable();
  void bind(const std::vector<FileSystem *> & fs);
  const std::vector<SerialOrganizedTableFilePtr>& getSerialPaths() const
  {
    return mSerialPaths;
  }
};

/**
 * A description of the on disk format of a table file together with
 * computed column expressions.  As we update minor version, it may become
 * necessary to add computed column expressions to allow integration of prior
 * versions with the updated schema.
 */
class TableFileMetadata
{
public:
  typedef std::map<std::string, std::string> computed_columns_type;
  typedef std::map<std::string, std::string>::const_iterator computed_columns_iterator;
private:
  /**
   * Stored columns.
   */
  std::string mRecordType;
  /**
   * Computed columns.
   */
  std::map<std::string, std::string> mComputedColumns;
public:
  TableFileMetadata(const std::string& recordType,
		    const std::map<std::string, std::string>& computedColumns);
  const RecordType * getRecordType(DynamicRecordContext & ctxt) const;
  const std::string& getRecordType() const 
  {
    return mRecordType;
  }
  const std::map<std::string, std::string>& getComputedColumns() const;
};

/**
 * A version of a table comprises one or more files.  Each file contains a 
 * group of columns for the table; some columns are stored and others
 * are computed..  If there is a primary key and version then
 * those columns must appear in every file.
 */
class TableColumnGroup
{
public:
  typedef std::map<int32_t, TableFileMetadata*>::iterator file_iterator;
  typedef std::map<int32_t, TableFileMetadata*>::const_iterator const_file_iterator;
private:
  // Table I belong to
  const class TableMetadata * mTable;
  // RecordType of this column group
  std::string mRecordType;
  // Map from table metadata to a minor version.
  std::map<int32_t, TableFileMetadata*> mMinorVersions;
  // Name of this column group
  std::string mName;
public:
  TableColumnGroup(const class TableMetadata * table,
		   const std::string& columnGroupType,
		   const std::string& columnGroupName);
  ~TableColumnGroup();

  bool canPrune() const 
  {
    // TODO: Support logic that makes it safe to prune
    // column groups from selects.
    return false;
  }

  const class TableMetadata * getTable() const
  {
    BOOST_ASSERT(mTable != NULL);
    return mTable;
  }

  const RecordType * getRecordType(DynamicRecordContext & ctxt) const;

  const std::vector<SortKey>& getSortKeys() const;
  const std::string& getName() const
  {
    return mName;
  }

  std::size_t getNumFiles() const 
  {
    return mMinorVersions.size();
  }
  void add(int32_t minorVersion, TableFileMetadata * m) 
  {
    mMinorVersions[minorVersion] = m;
  }
  const_file_iterator findFiles(int32_t minorVersion) const
  {
    return mMinorVersions.find(minorVersion);
  }
  const_file_iterator endFiles() const
  {
    return mMinorVersions.end();
  }
  file_iterator beginFiles()
  {
    return mMinorVersions.begin();
  }
  file_iterator endFiles()
  {
    return mMinorVersions.end();
  }
};

class TableMetadata
{
public:
  typedef std::vector<SortKey>::const_iterator sort_key_const_iterator;
  typedef std::vector<TableColumnGroup*>::const_iterator column_group_const_iterator;
private:
  std::string mTableName;
  std::string mRecordType;
  std::vector<SortKey> mSortKeys;
  // Optional primary key.  A table with multiple 
  // column groups must have a primary key and must be sorted
  // its primary key.  There is an implicit assumption (which we 
  // cannot validate) that the table is partitioned on a prefix
  // of its primary key.
  std::vector<std::string> mPrimaryKey;
  // Column containing the version of the record.
  // If the version column is not empty then there must be a 
  // primary key.
  std::string mVersion;
  // Column groups for the table.  Every column in the table
  // is associated with exactly one column group.
  // We do NOT support moving columns between column groups.
  std::vector<TableColumnGroup*> mColumnGroups;
  // If we have multiple column groups then they must be
  // named.  This is the map from name to column group.
  std::map<std::string, TableColumnGroup*> mColumnGroupNames;
  // (Default) compression for table data
  CompressionType mCompressionType;
  // (Default) text/binary format
  TableFileFormat mFileFormat;
  
public:
  TableMetadata(const std::string& tableName,
		const std::string& recordType,
		const std::vector<SortKey>& sortKeys,
                const CompressionType & compressionType,
                const TableFileFormat & tableFormat=TableFileFormat());

  TableMetadata(const std::string& tableName,
		const std::string& recordType,
		const std::vector<SortKey>& sortKeys,
                const CompressionType & compressionType,
		const std::vector<std::string>& primaryKey,
		const std::string& version);

  ~TableMetadata();

  /**
   * Given a path into a serial organized table, identify
   * the coresponding metadata.
   */
  void resolveMetadata(SerialOrganizedTableFilePtr file,
		       TableColumnGroup* & tableColumnGroup,
		       TableFileMetadata* & fileMetadata) const;

  /**
   * Add a single column group with all of the table columns.
   */
  TableColumnGroup * addDefaultColumnGroup();

  /**
   * Add a named column group.   The name of the column group
   * corresponds to the second component in the file system
   * path (e.g. MinorVersion/CgName/Batch).
   */
  TableColumnGroup * addColumnGroup(const std::string& cgName,
				    const std::string& cgRecordType);

  column_group_const_iterator beginColumnGroups() const
  {
    return mColumnGroups.begin();
  }

  column_group_const_iterator endColumnGroups() const
  {
    return mColumnGroups.end();
  }

  /**
   * The offical table format of the table.
   */
  const RecordType * getRecordType(DynamicRecordContext& ctxt) const;
  /**
   * Is the data stored in a sorted order?
   */
  const std::vector<SortKey>& getSortKeys() const;

  /**
   * Optional primary key for the table?
   */
  const std::vector<std::string>& getPrimaryKey() const;
  std::vector<SortKey> getPrimarySortKey() const;

  /**
   * Version column for the table.
   */
  const std::string& getVersion() const;

  /**
   * Compression type of the table
   */
  const CompressionType& getCompressionType() const;

  /**
   * The underlying file format
   */
  const TableFileFormat& getTableFormat() const;

  /**
   * Add columns required for column group processing.
   */
  void addColumnGroupRequired(std::set<std::string>& s) const;
};

class MetadataCatalog
{
public:
  typedef std::shared_ptr<const TableMetadata> ptr_type;
  typedef std::map<std::string, ptr_type> map_type;
private:
  map_type mCatalog;
public:
  MetadataCatalog();
  ~MetadataCatalog();
  ptr_type find(const std::string& tableName) const;
};

#endif
