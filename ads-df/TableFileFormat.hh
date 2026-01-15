/**
 * Copyright (c) 2026, Akamai Technologies
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

#ifndef __TABLEFILEFORMAT_HH__
#define __TABLEFILEFORMAT_HH__

#include <string>

#include <boost/serialization/nvp.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>

/**
 * Format of a file storing Trecul table data (currently just delimited text and native binary).
 */
class TableFileFormat
{
private:
  std::string mExtension;
  TableFileFormat(const char * extension)
    :
    mExtension(extension)
  {
  }

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mExtension);
  }
public:
  TableFileFormat()
    :
    mExtension("txt")
  {
  }
  bool operator==(const TableFileFormat & rhs) const
  {
    return extension() == rhs.extension();
  }
  bool operator!=(const TableFileFormat & rhs) const
  {
    return extension() != rhs.extension();
  }
  const std::string& extension() const
  {
    return mExtension;
  }
  /**
   * TAB and newline delimited text
   */
  static TableFileFormat Delimited()
  {
    return TableFileFormat("txt");
  }
  /** 
   * Native Trecul serialization.
   */
  static TableFileFormat Binary()
  {
    return TableFileFormat("bin");
  }
};


#endif

