#include "remain_qps.h"
#include "options.h"
#include <atomic>
#include <cstdint>
#include <iostream>
#include <mysql/mysql.h>
#include <thread>
#include <unistd.h>

extern uint detail_log;
extern char *database;
extern uint64_t report_interval;
extern uint64_t test_time;
extern uint64_t concurrency;
extern uint64_t test_qps;

static Statistics qps_state;
static Statistics qps_per_second;
static std::atomic<uint32_t> active_threads{0};
static std::atomic<time_t> pre_time{0};

static std::atomic_bool can_continue{true};
static std::atomic_bool should_quit{false};

void RemainQPSTest::run(const std::vector<std::string> &querys) {
  while (!should_quit) {
    if (can_continue) {
      conns_prepare();
      basic_query(querys);
      conns_close();
    } else {
      usleep(1);
    }
  }
}

int RemainQPSTest::basic_query(const std::vector<std::string> &querys) {
  int res = 0;
  MYSQL_RES *mysql_res = nullptr;
  MYSQL_ROW row;

  for (auto &query : querys) {
    if (!can_continue) {
      break;
    }
    res = mysql_query(m_conn_, query.data());
    if (res != 0) {
      qps_state.increase_cnt_failed();
      qps_per_second.increase_cnt_failed();
      std::cout << "Failed to test consistency, sql: " << query
                << ", errno: " << mysql_errno(m_conn_)
                << ", errmsg: " << mysql_error(m_conn_);
      return -1;
    }
    qps_state.increase_cnt_total();
    qps_per_second.increase_cnt_total();

    mysql_res = mysql_store_result(m_conn_);
    mysql_free_result(mysql_res);
  }

  return res;
}

void start_detect_qps() {
  while (!should_quit) {
    time_t now_time = time(nullptr);
    if (now_time == pre_time) {
      if (qps_per_second.get_cnt_total() >= test_qps) {
        can_continue.store(false);
      }
    } else {
      qps_per_second.clear();
      can_continue.store(true);
      pre_time = now_time;
    }
    usleep(1);
  }
}

void start_remain_qps_test(int thread_id,
                           const std::vector<std::string> &querys) {
  if (detail_log) {
    std::cout << "start thread: " << thread_id << std::endl;
  }

  RemainQPSTest t(database, 0, test_qps);
  t.run(querys);
  t.cleanup();
  active_threads--;

  if (detail_log) {
    std::cout << "stop thread: " << thread_id << std::endl;
  }
  return;
}

static void print_result_interval() {

  Statistics new_state, pre_state;
  pre_state = qps_state;

  time_t start_time = time(NULL);
  time_t end_time;
  if (report_interval != 0) {
    while (active_threads.load() != 0) {
      sleep(report_interval);
      new_state = qps_state;
      std::cout << "qps: "
                << (new_state.get_cnt_total() - pre_state.get_cnt_total()) /
                       report_interval;
      std::cout << ", failed qps: "
                << (new_state.get_cnt_failed() - pre_state.get_cnt_failed()) /
                       report_interval;
      std::cout << ", active threads : " << active_threads.load() << std::endl;

      pre_state = new_state;
      end_time = time(NULL);
      uint64_t run_time = end_time - start_time;
      if (run_time >= test_time) {
        should_quit.store(true);
        break;
      }
    }
  }
}

static void print_result_summarize() {
  std::cout << "mean qps in all time: " << qps_state.get_cnt_total() / test_time
            << ", failed cnt: " << qps_state.get_cnt_failed() / test_time
            << std::endl;
}

int main_remain_qps() {
  std::thread *ct_threads[concurrency];
  std::vector<std::string> querys = get_querys_from_file();
  pre_time = time(nullptr);

  std::thread *detect_qps_thread = new std::thread(start_detect_qps);

  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id] =
        new std::thread(start_remain_qps_test, thread_id, querys);
    active_threads++;
  }

  print_result_interval();

  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id]->join();
    delete ct_threads[thread_id];
  }

  detect_qps_thread->join();
  delete detect_qps_thread;

  print_result_summarize();

  return 0;
}

