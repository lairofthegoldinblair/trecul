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

#include <filesystem>
#include <stdexcept>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include "IQLInterpreter.hh"
#include "RecordType.hh"
#include "TableMetadata.hh"

SerialOrganizedTable::SerialOrganizedTable()
{
}  

SerialOrganizedTable::SerialOrganizedTable(int32_t commonVersion,
					   int32_t tableMajorVersion,
					   const std::string& tableName,
					   const char * pred)
  :
  mCommonVersion(commonVersion),
  mTableMajorVersion(tableMajorVersion),
  mTableName(tableName),
  mContext(NULL),
  mRecordType(NULL),
  mPredicate(NULL),
  mMinorVersionField(0),
  mDateField(1),
  mRuntimeContext(NULL)
{
  // These are the path components of the table.
  // We assume that it is possible to write
  // predicates against all of these.
  mPathComponents.push_back("MinorNumber");
  mPathComponents.push_back("Date");
  mPathComponents.push_back("BatchId");
  
  // Create a record type against which we evaluate predicates.
  mContext = new DynamicRecordContext();
  std::vector<RecordMember> members;
  members.push_back(RecordMember("MinorNumber", VarcharType::Get(*mContext)));
  members.push_back(RecordMember("Date", VarcharType::Get(*mContext)));
  members.push_back(RecordMember("BatchId", VarcharType::Get(*mContext)));
  mRecordType = RecordType::get(*mContext, members);

  mFields.push_back(mRecordType->getFieldAddress("MinorNumber"));
  mFields.push_back(mRecordType->getFieldAddress("Date"));
  mFields.push_back(mRecordType->getFieldAddress("BatchId"));
  mMinorVersionField = 0;
  mDateField = 1;

  std::vector<RecordMember> emptyMembers;
  const RecordType * emptyTy = RecordType::get(*mContext, emptyMembers);
  std::vector<const RecordType *> types;
  types.push_back(mRecordType);
  types.push_back(emptyTy);

  if (pred) {
    mPredicate = new RecordTypeFunction(*mContext, "chareq", types, pred);
    mRuntimeContext = new InterpreterContext();
  }
}

SerialOrganizedTable::~SerialOrganizedTable()
{
  delete mContext;
  delete mPredicate;
  // Note: mRecordType owned by mContext.
  delete mRuntimeContext;
}

void SerialOrganizedTable::bindComponent(FileSystem * fs,
					 std::size_t level,
					 PathPtr p)
{
  std::vector<std::shared_ptr<FileStatus> > ls;
  fs->list(p, ls);
  if (level + 1 == mPathComponents.size()) {
    for(std::vector<std::shared_ptr<FileStatus> >::iterator pit = ls.begin();
	pit != ls.end();
	++pit) {
      // Extract the path elements and evaluate predicate against them.
      // TODO: Support more that VARCHAR data type.
      std::filesystem::path fsPath((*pit)->getPath()->getUri()->getPath());
      if (std::size_t(std::distance(fsPath.begin(), fsPath.end())) < 
	  mPathComponents.size()) {
	throw std::runtime_error("Invalid table path.  Too short to evaluate predicate");
      }
      // Set the last path components into the fields in reverse order.
      std::filesystem::path::iterator comp = fsPath.end();
      --comp;
      // We expect to have a trailing slash in the URI hence a trailing empty component in
      // the std::filesystem path.  Skip it if it is there.
      if (comp->empty()) {
	--comp;
      }
      RecordBuffer buf = mRecordType->getMalloc().malloc();
      for(std::vector<FieldAddress>::const_reverse_iterator field = mFields.rbegin();
	  field != mFields.rend();
	  ++field) {
	std::filesystem::path tmp = *comp;
	field->SetVariableLengthString(buf, comp->string().c_str(), comp->string().size());
	--comp;
      }
      bool good = mPredicate ?
	mPredicate->execute(buf, RecordBuffer(), mRuntimeContext) : true;
      const char * minorVersionStr = mFields[mMinorVersionField].getVarcharPtr(buf)->c_str();
      int32_t minorVersion = boost::lexical_cast<int32_t>(minorVersionStr);
      std::string dateStr(mFields[mDateField].getVarcharPtr(buf)->c_str());
      mRecordType->getFree().free(buf);
      if (good)
	mSerialPaths.push_back(SerialOrganizedTableFile::get(minorVersion, dateStr,
							     Path::get(p, (*pit)->getPath())));
    }
  } else {
    for(std::vector<std::shared_ptr<FileStatus> >::iterator pit = ls.begin();
	pit != ls.end();
	++pit) {
      // TODO: For predicates that don't reference all path components
      // evaluate as soon as possible to avoid directory expansion.
      // Such logic could be very fancy and extract portions of a single
      // predicate that could be evaluated at each level...
      if ((*pit)->isDirectory())
	bindComponent(fs, level+1, Path::get(p, (*pit)->getPath()));
    }
  }
}

PathPtr SerialOrganizedTable::getTableRoot(FileSystem * fs)
{
  // We recursively descend into the table root 
  // First layer is minor version
  // Second layer is date
  // Third layer is batch id.
  // Last layer is actual files.

  // First we locate a directory named /CommonVersion_*
  // Then we have the table root.
  std::vector<std::shared_ptr<FileStatus> > ls;
  fs->list(fs->getRoot(), ls);

  // Find the pattern /CommonVersion, should not be more than one match.
  // We look for the pattern relative to the URI base of the filesystem.
  std::string match = boost::lexical_cast<std::string>(mCommonVersion);
  match += "_"; 
  PathPtr rootPath;
  for(std::vector<std::shared_ptr<FileStatus> >::iterator it=ls.begin();
      it != ls.end();
      ++it) {
    std::filesystem::path fsPath((*it)->getPath()->getUri()->getPath());
    // Get the last component of the fsPath. Skip an empty path component (standing for trailing
    // slash) if present.
    std::filesystem::path::iterator comp = fsPath.end();
    --comp;
    if (comp->string().empty()) {
      --comp;
    }
    std::string tmp = comp->string();
    if (tmp.size() >= match.size() && 
	boost::algorithm::equals(match, tmp.substr(0,match.size()))) {
      rootPath = (*it)->getPath();
      break;
    }
  }
  if (rootPath == PathPtr()) {
    throw std::runtime_error("Could not resolve table base");
  }
  PathPtr nextPath = Path::get(rootPath,
			       mTableName + "/");
  return Path::get(nextPath,
                   boost::lexical_cast<std::string>(mTableMajorVersion) + "/");
}

void SerialOrganizedTable::bind(const std::vector<FileSystem *> & filesystems)
{
  for(auto fs : filesystems) {
    bindComponent(fs, 0, getTableRoot(fs));
  }
}

void SerialOrganizedTable::getSerialFiles(FileSystem * fs,
					  int32_t serialNumber,
					  std::vector<std::shared_ptr<FileChunk> >& files) const
{
  std::ostringstream ss;
  ss << "serial_" << std::setw(5) << std::setfill('0') << serialNumber << ".gz";
  std::string sn(ss.str());
  for(std::vector<SerialOrganizedTableFilePtr>::const_iterator it = getSerialPaths().begin();
      it != getSerialPaths().end();
      ++it) {
    typedef std::vector<std::shared_ptr<FileStatus> > fstats;
    fstats ls;
    fs->list((*it)->getPath(), ls);
    for(fstats::iterator fit = ls.begin();
	fit != ls.end();
	++fit) {
      const std::string& fname((*fit)->getPath()->toString());
      if (fname.size() >= sn.size() &&
	  boost::algorithm::equals(sn, fname.substr(fname.size()-sn.size()))) {
	files.push_back(std::make_shared<FileChunk>(fname, 
						      0,
						      std::numeric_limits<uint64_t>::max()));
      }
    }
  }  
}

TableFileMetadata::TableFileMetadata(const std::string& recordType,
				     const std::map<std::string, std::string>& computedColumns)
  :
  mRecordType(recordType),
  mComputedColumns(computedColumns)
{
}

const RecordType * TableFileMetadata::getRecordType(DynamicRecordContext & ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

const std::map<std::string, std::string>& TableFileMetadata::getComputedColumns() const
{
  return mComputedColumns;
}

TableMetadata::TableMetadata(const std::string& tableName,
			     const std::string& recordType,
			     const std::vector<SortKey>& sortKeys,
                             const CompressionType & compressionType)
  :
  mTableName(tableName),
  mRecordType(recordType),
  mSortKeys(sortKeys),
  mCompressionType(compressionType)
{
}

TableMetadata::TableMetadata(const std::string& tableName,
			     const std::string& recordType,
			     const std::vector<SortKey>& sortKeys,
                             const CompressionType & compressionType,
			     const std::vector<std::string>& primaryKey,
			     const std::string& version)
  :
  mTableName(tableName),
  mRecordType(recordType),
  mSortKeys(sortKeys),
  mPrimaryKey(primaryKey),
  mVersion(version),
  mCompressionType(compressionType)
{
  if (sortKeys.size() < primaryKey.size()) { 
    throw std::runtime_error("Primary key must be a prefix of sort keys");
  }
  for(std::size_t i = 0, e = primaryKey.size(); i<e; ++i) {
    if (primaryKey[i] != sortKeys[i].getName()) {
      throw std::runtime_error("Primary key must be a prefix of sort keys");
    }
  }
  if (version.size() && 0 == primaryKey.size()) {
    throw std::runtime_error("Versioned table requires a primary key");
  }
}

TableMetadata::~TableMetadata()
{
  for(std::vector<TableColumnGroup*>::iterator it = mColumnGroups.begin();
      it != mColumnGroups.end();
      ++it) {
    delete *it;
  }
}

void TableMetadata::resolveMetadata(SerialOrganizedTableFilePtr file,
				    TableColumnGroup* & tableColumnGroup,
				    TableFileMetadata* & fileMetadata) const
{
  // HACK: The code is calling the second component
  // of the path the "Date" because that is what it
  // has been historically.  We are piggybacking on that
  // path component to name the column group.  This 
  // means that we cannot support date partitioned tables
  // with multiple column groups without some additional
  // work on metadata.
  const std::string& cgName = file->getDate();
  if (mColumnGroupNames.size() == 0) {
    tableColumnGroup = mColumnGroups[0];
  } else {
    std::map<std::string, TableColumnGroup*>::const_iterator it = 
      mColumnGroupNames.find(cgName);
    if (it == mColumnGroupNames.end()) {
      throw std::runtime_error((boost::format("Invalid path in file system: '%1%'; "
					      "contains unknown column group name: '%2%'")
				% file->getPath()->toString() % cgName).str());
    }
    tableColumnGroup = it->second;
  }

  int32_t minorVersion = file->getMinorVersion();
  TableColumnGroup::const_file_iterator f = tableColumnGroup->findFiles(minorVersion);
  if (f == tableColumnGroup->endFiles()) {
    throw std::runtime_error("Invalid serial table path");
  }
  fileMetadata = f->second;
}

TableColumnGroup * TableMetadata::addDefaultColumnGroup()
{
  BOOST_ASSERT(mColumnGroups.size() == 0);
  mColumnGroups.push_back(new TableColumnGroup(this, mRecordType, ""));
  return mColumnGroups.back();
}

TableColumnGroup * TableMetadata::addColumnGroup(const std::string& cgName,
						 const std::string& cgRecordType)
{
  if (mColumnGroupNames.find(cgName) != mColumnGroupNames.end()) {
    throw std::runtime_error("INTERNAL ERROR: duplicate column group name");
  }
  if (mColumnGroupNames.size() != mColumnGroups.size()) {
    throw std::runtime_error("INTERNAL ERROR: cannot use default column group "
			     "and named column groups in same table");
  }
  mColumnGroups.push_back(new TableColumnGroup(this, cgRecordType, cgName));
  mColumnGroupNames[cgName] = mColumnGroups.back();
  return mColumnGroups.back();
}

const RecordType * TableMetadata::getRecordType(DynamicRecordContext& ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

const std::vector<SortKey>& TableMetadata::getSortKeys() const
{
  return mSortKeys;
}

const std::vector<std::string>& TableMetadata::getPrimaryKey() const
{
  return mPrimaryKey;
}

std::vector<SortKey> TableMetadata::getPrimarySortKey() const
{
  return std::vector<SortKey>(mSortKeys.begin(), mSortKeys.begin() + mPrimaryKey.size());
}

const CompressionType& TableMetadata::getCompressionType() const
{
  return mCompressionType;
}

const std::string& TableMetadata::getVersion() const
{
  return mVersion;
}

void TableMetadata::addColumnGroupRequired(std::set<std::string>& s) const
{
  for(std::vector<std::string>::const_iterator pk = mPrimaryKey.begin(),
	e = mPrimaryKey.end(); pk != e; ++pk) {
    if (s.find(*pk) == s.end()) {
      s.insert(*pk);
    }
  }
  if (mVersion.size() && s.find(mVersion) == s.end()) {
    s.insert(mVersion);
  }    
}

TableColumnGroup::TableColumnGroup(const class TableMetadata * table,
				   const std::string& recordType,
				   const std::string& columnGroupName)
  :
  mTable(table),
  mRecordType(recordType),
  mName(columnGroupName)
{
  BOOST_ASSERT(mTable != NULL);
}

TableColumnGroup::~TableColumnGroup()
{
  for(file_iterator f = beginFiles(), e = endFiles(); 
      f != e; ++f) {
    delete f->second;
  }
}

const std::vector<SortKey>& TableColumnGroup::getSortKeys() const
{
  return mTable->getSortKeys();
}

const RecordType * TableColumnGroup::getRecordType(DynamicRecordContext & ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

MetadataCatalog::MetadataCatalog()
{
}

MetadataCatalog::~MetadataCatalog()
{
}

MetadataCatalog::ptr_type
MetadataCatalog::find(const std::string& tableName) const
{
  map_type::const_iterator it = mCatalog.find(tableName);
  if (it == mCatalog.end()) return MetadataCatalog::ptr_type();
  return it->second;
}

