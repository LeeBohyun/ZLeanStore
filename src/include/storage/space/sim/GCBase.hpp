// gc_base.hpp
#pragma once
#include <string>

namespace leanstore::storage::space{
class GCBase {
public:
    virtual ~GCBase() = default;
    virtual void writePage(uint64_t pid) = 0;
    virtual std::string name() = 0;
};
}

