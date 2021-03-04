#pragma once
#if defined(__cplusplus) && __cplusplus >= 201703L && defined(__has_include)
#if __has_include(<filesystem>)
#define GHC_USE_STD_FS
#include <filesystem>
namespace fs = std::filesystem;
#endif
#endif
#ifndef GHC_USE_STD_FS
#include <ghc/filesystem.hpp>
namespace fs = ghc::filesystem;
#endif

constexpr fs::perms generalPermission = 
      fs::perms::owner_all | 
      fs::perms::group_read | fs::perms::group_exec | 
      fs::perms::others_read | fs::perms::others_exec;

inline bool createDir(const fs::path &dir, std::error_code &ec, fs::perms permission = generalPermission) {
  if (fs::exists(dir) && fs::is_directory(dir)) return true;
  if (!fs::create_directories(dir, ec)) return false;
  fs::permissions(dir, permission, ec);
  return !ec;
}