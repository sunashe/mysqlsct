/*
 * @Date         : 2023-03-29 15:21:00
 * @Author       : huyutuo.hyt
 * @FilePath     : short_connection.cc
 * @Description  :
 */

#include "options.h"
#include "short_connection.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

extern char *user;
extern char *password;
extern char *database;
extern char *host_ro;
extern char *host_rw;
extern char *host;
extern uint port;
extern uint port_rw;
extern uint port_ro;
extern uint sc_gap_us;
extern uint table_cnt;
extern uint64_t concurrency;
extern uint64_t table_size;
extern uint64_t iterations;
extern uint64_t report_interval; // s
extern uint detail_log;
extern uint64_t sleep_after_sct_failed; // s
extern uint select_after_insert;
extern uint short_connection;
extern std::string table_name_prefix;

static std::atomic<uint32_t> active_threads{0};
static Statistics state;

int ShortConnnectionTest::conns_prepare() {
  int res = 0;
  do {
    // Connect to mysql
    m_conn_ = mysql_init(0);
    if (m_conn_ == nullptr) {
      std::cerr << "Failed to init m_conn_ " << std::endl;
      res = -1;
      break;
    }

    if (!mysql_real_connect(m_conn_, host, user, password, m_db_name_, port,
                            nullptr, 0)) {
      std::cerr << "Failed to connect to MySql."
                << " errno: " << mysql_errno(m_conn_)
                << ",errmsg: " << mysql_error(m_conn_) << std::endl;
      res = -1;
      break;
    }
  } while (0);

  return res;
}

void ShortConnnectionTest::conns_close() {
  if (m_conn_ != nullptr) {
    mysql_close(m_conn_);
    m_conn_ = nullptr;
  }
}

void ShortConnnectionTest::run(const std::vector<std::string> &querys) {
  while (m_times_++ < iterations) {
    conns_prepare();
    if (basic_query(querys) == 0) {
      state.increase_cnt_total();
    } else {
      state.increase_cnt_failed();
    }
    conns_close();
  }
}

int ShortConnnectionTest::basic_query(const std::vector<std::string> &querys) {
  int res = 0;
  MYSQL_RES *mysql_res = nullptr;
  MYSQL_ROW row;

  for (auto &query : querys) {
    res = mysql_query(m_conn_, query.data());
    if (res != 0) {
      std::cout << "Failed to test consistency, sql: " << query
                << ", errno: " << mysql_errno(m_conn_)
                << ", errmsg: " << mysql_error(m_conn_);
      return -1;
    }

    mysql_res = mysql_store_result(m_conn_);
    mysql_free_result(mysql_res);
  }

  return res;
}

int ShortConnnectionTest::cleanup() {
  if (m_conn_ != nullptr) {
    mysql_close(m_conn_);
    m_conn_ = nullptr;
  }

  mysql_thread_end();
  return 0;
}

void start_short_connection_test(int thread_id,
                                 const std::vector<std::string> &querys) {
  if (detail_log) {
    std::cout << "start thread: " << thread_id << std::endl;
  }
  ShortConnnectionTest t(database, 0);
  t.run(querys);
  t.cleanup();
  active_threads--;

  if (detail_log) {
    std::cout << "stop thread: " << thread_id << std::endl;
  }
  return;
}

void show_querys(const std::vector<std::string> &querys) {
  std::cout << "------------ querys -------------" << std::endl;
  for (auto &query : querys) {
    std::cout << query << std::endl;
  }
  std::cout << "---------------------------------\n" << std::endl;
}

std::vector<std::string> get_querys_from_file() {
  std::vector<std::string> lines;
  std::ifstream ifs;
  ifs.open("short_connection_querys.txt");
  if (ifs.is_open()) {
    std::string line;
    while (std::getline(ifs, line)) {
      lines.push_back(line);
    }
    ifs.close();
  }

  show_querys(lines);
  return lines;
}

static void print_result_interval() {
  Statistics new_state, pre_state;
  pre_state = state;

  if (report_interval != 0) {
    while (active_threads.load() != 0) {
      sleep(report_interval);
      new_state = state;
      std::cout << "cd(connect/disconnect)ps: "
                << (new_state.get_cnt_total() - pre_state.get_cnt_total()) /
                       report_interval;
      std::cout << ", failed cdps: "
                << (new_state.get_cnt_failed() - pre_state.get_cnt_failed()) /
                       report_interval;
      std::cout << ", active threads : " << active_threads.load() << std::endl;
      pre_state = new_state;
    }
  }
}

static void print_result_summarize() {
  std::cout << "Test connection/disconnect cnt: " << state.get_cnt_total()
            << ", failed cnt: " << state.get_cnt_failed() << std::endl;
}

int main_shortct() {
  std::thread *ct_threads[concurrency];
  std::vector<std::string> querys = get_querys_from_file();
  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id] =
        new std::thread(start_short_connection_test, thread_id, querys);
    active_threads++;
  }

  print_result_interval();

  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id]->join();
    delete ct_threads[thread_id];
  }

  print_result_summarize();

  return 0;
}
