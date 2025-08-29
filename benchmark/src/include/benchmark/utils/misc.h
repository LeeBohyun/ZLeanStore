#pragma once

#include <cstdint>
#include <sys/resource.h>

struct FreeDelete {
  void operator()(void *x);
};

auto RoundUp(uint64_t align, uint64_t num_to_round) -> uint64_t;
auto Rdtsc() -> uint64_t;
void SetStackSize(uint64_t stack_size_in_mb);
