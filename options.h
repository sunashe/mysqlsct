#ifndef OPTION_H
#define OPTION_H

#include <atomic>
#include <cstdint>

void usage();
bool parse_option(int argc, char *argv[]);
bool verify_variables();
void free_option();

enum TestMode {
  CONSISTENT,
  SHORT_CONNECT,
  REMAIN_QPS,
};

class Statistics {
public:
  Statistics() {
    m_cnt.store(0);
    m_cnt_failed.store(0);
  }
  ~Statistics() {}

  void operator=(const Statistics &state) {
    m_cnt.store(state.m_cnt.load());
    m_cnt_failed.store(state.m_cnt_failed.load());
  }

  uint64_t get_cnt_total() { return m_cnt.load(); }

  uint64_t get_cnt_failed() { return m_cnt_failed.load(); }

  void increase_cnt_total() { m_cnt++; }

  void increase_cnt_failed() { m_cnt_failed++; }

  void clear() {
    m_cnt.store(0);
    m_cnt_failed.store(0);
  }

private:
  std::atomic<uint64_t> m_cnt{0};
  std::atomic<uint64_t> m_cnt_failed{0};
};

#endif
