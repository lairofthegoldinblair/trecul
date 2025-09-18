#include <map>
#include <stdexcept>
#include <vector>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/format.hpp>
#include "CompressionType.hh"

const CompressionType & CompressionType::fromString(const std::string & ty, std::error_code & ec)
{
  static std::vector<CompressionType> types { Uncompressed(), Gzip(), Zstandard() };
  static std::map<std::string, std::size_t> type_strings { { "none", 0 }, { "uncompressed", 0 }, { "txt", 0 },
                                                           { "zlib", 1 }, { "gzip", 1 }, { "gz", 1 },
                                                           { "zstd", 2 }, { "zstandard", 2 }, { "zst", 2 } };

  auto it = type_strings.find(boost::algorithm::to_lower_copy(ty));
  if (type_strings.end() == it) {
    ec = std::error_code(EINVAL, std::generic_category());
    return types[0];
  }
  ec = std::error_code();
  return types[it->second];
}

const CompressionType & CompressionType::fromString(const std::string & ty)
{
  std::error_code ec;
  const CompressionType & ret { fromString(ty, ec) };
  if (ec) {
    throw std::runtime_error((boost::format("Invalid compression type \"%1%\"") % ty).str());
  }
  return ret;
}
