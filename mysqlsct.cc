/**
 * @file mysqlsct.cc
 * @author sunashe
 * @date 9/13/22
 * @version 0.0.1
 **/
#include <atomic>
#include <cstring>
#include <getopt.h>
#include <iostream>
#include <mysql/mysql.h>
#include <string>
#include <thread>
#include <unistd.h>

#include "options.h"
#include "remain_qps.h"
#include "short_connection.h"

using std::string;

std::atomic<uint32_t> active_threads{0};
std::atomic<uint32_t> running_threads{0};
std::atomic<uint64_t> processed_times{0};

extern char *user;
extern char *password;
extern char *database;
extern char *host_ro;
extern char *host_rw;
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
extern bool skip_prepare;

extern TestMode test_mode;

int main_sct();

int main(int argc, char *argv[]) {
  int ret = 0;
  if (!parse_option(argc, argv)) {
    free_option();
    return 1;
  }

  if (!verify_variables()) {
    free_option();
    return 1;
  }
  mysql_library_init(0, NULL, NULL);
  if (test_mode == TestMode::CONSISTENT) {
    main_sct();
  } else if (test_mode == TestMode::SHORT_CONNECT) {
    main_shortct();
  } else if (test_mode == TestMode::REMAIN_QPS) {
    main_remain_qps();
  } else {
    std::cout << "wrong mode : " << test_mode << std::endl;
  }

  mysql_library_end();
  free_option();
  return ret;
}



static Statistics state;

class TestC {
public:
  TestC(const char *db_name, string table_name, uint64_t times,
        uint64_t table_size, uint64_t thread_id) {
    m_db_name_ = db_name;
    m_table_name_ = table_name;
    m_times_ = times;
    m_table_size_ = table_size;
    m_thread_id_ = thread_id;
  }

  int run();
  int cleanup();

private:
  int conns_prepare();
  void conns_close();
  int data_prepare();
  int test_select_after_insert(const uint64_t pk);
  int commit_conn_trx(MYSQL *conn);
  int insert_test(uint64_t pk);
  int update(uint64_t &pk, uint64_t &old_value, uint64_t &new_value);
  int consistency_test(uint64_t pk, uint64_t old_value, uint64_t expected);

  const char *m_db_name_;
  string m_table_name_;
  uint64_t m_times_;
  uint64_t m_table_size_;
  uint64_t m_thread_id_;

  MYSQL *m_conn_rw_{nullptr};
  MYSQL *m_conn_ro_{nullptr};
};

void start_test(int thread_id) {
  if (detail_log) {
    std::cout << "start thread: " << thread_id << std::endl;
  }
  string table_name = table_name_prefix + std::to_string(thread_id);
  TestC t(database, table_name, iterations, table_size, thread_id);
  t.run();
  t.cleanup();
  active_threads--;

  if (detail_log) {
    std::cout << "stop thread: " << thread_id << std::endl;
  }
  return;
}

int TestC::data_prepare() {
  int res = 0;
  string query = "drop table if exists " + m_table_name_;
  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cout << "Failed to drop table, sql: " << query
              << ", errno: " << mysql_errno(m_conn_rw_)
              << ", errmsg: " << mysql_error(m_conn_rw_) << std::endl;
  }

  query = "create table " + m_table_name_ +
          " (id bigint not null primary key, c1 bigint)";
  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cout << "Failed to create table, sql: " << query
              << ", errno: " << mysql_errno(m_conn_rw_)
              << ", errmsg: " << mysql_error(m_conn_rw_) << std::endl;
  }

  for (uint64_t pk = 1; pk <= m_table_size_; pk++) {
    query = "insert into " + m_table_name_ + " values(" + std::to_string(pk) +
            "," + "0)";
    res = mysql_query(m_conn_rw_, query.data());
    if (res != 0) {
      std::cout << "Failed to insert, sql: " << query
                << ", errno: " << mysql_errno(m_conn_rw_)
                << ", errmsg: " << mysql_error(m_conn_rw_) << std::endl;
      return res;
    }

    if (select_after_insert) {
      res = test_select_after_insert(pk);
    }
    if (res != 0) {
      return res;
    }
  }

  query = "select count(*) from " + m_table_name_;
  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cout << "Failed to check the table size, errno: "
              << mysql_errno(m_conn_rw_)
              << ", errmsg: " << mysql_error(m_conn_rw_) << std::endl;
    return res;
  }
  MYSQL_RES *mysql_res = nullptr;
  MYSQL_ROW row;
  mysql_res = mysql_store_result(m_conn_rw_);
  row = mysql_fetch_row(mysql_res);
  if (row[0] == nullptr) {
    std::cout << "Failed to check the table size." << std::endl;
  }
  uint64_t table_size = strtoull(row[0], nullptr, 10);
  if (table_size != m_table_size_) {
    std::cout << "Failed to check the table size, " << m_table_name_
              << " should be: " << m_table_size_ << ", but " << table_size
              << std::endl;
  }
  mysql_free_result(mysql_res);

  return res;
}

void TestC::conns_close() {
  if (m_conn_rw_ != nullptr) {
    mysql_close(m_conn_rw_);
    m_conn_rw_ = nullptr;
  }

  if (m_conn_ro_ != nullptr) {
    mysql_close(m_conn_ro_);
    m_conn_ro_ = nullptr;
  }
}

int TestC::conns_prepare() {
  int res = 0;
  do {
    // Connect to RW
    m_conn_rw_ = mysql_init(0);
    if (m_conn_rw_ == nullptr) {
      std::cerr << "Failed to init m_conn_rw_ " << std::endl;
      res = -1;
      break;
    }

    if (!mysql_real_connect(m_conn_rw_, host_rw, user, password, m_db_name_,
                            port_rw, nullptr, 0)) {
      std::cerr << "Failed to connect to RW."
                << " errno: " << mysql_errno(m_conn_rw_)
                << ",errmsg: " << mysql_error(m_conn_rw_) << std::endl;
      res = -1;
      break;
    }

    // Connection to RO
    m_conn_ro_ = mysql_init(0);
    if (m_conn_ro_ == nullptr) {
      std::cerr << "Failed to init m_conn_ro_ " << std::endl;
      res = -1;
      break;
    }

    if (!mysql_real_connect(m_conn_ro_, host_ro, user, password, m_db_name_,
                            port_ro, nullptr, 0)) {
      std::cout << "Failed to connect to RO."
                << " errno: " << mysql_errno(m_conn_ro_)
                << ",errmsg: " << mysql_error(m_conn_ro_);
      res = -1;
      break;
    }

    /* res = mysql_query(m_conn_ro_, "set session autocommit = 0"); */
    /* if (res != 0) { */
    /*   std::cout << "Failed to set session  autocommit = 0" << std::endl; */
    /* } */

  } while (0);

  return res;
}

int TestC::commit_conn_trx(MYSQL *conn) {
  int res = 0;
  res = mysql_query(conn, "commit");
  if (res != 0) {
    std::cerr << "Failed to commit, errno: " << mysql_errno(conn)
              << ", errmsg: " << mysql_error(conn);
  }

  return res;
}

int TestC::test_select_after_insert(const uint64_t pk) {
  int res = 0;
  string query;
  MYSQL_RES *mysql_res = nullptr;

  do {
    query =
        "select * from " + m_table_name_ + " where id = " + std::to_string(pk);
    res = mysql_query(m_conn_ro_, query.data());
    if (res != 0) {
      std::cout << "Failed to select after insert, sql: " << query
                << ", errno: " << mysql_errno(m_conn_ro_)
                << ", errmsg: " << mysql_error(m_conn_ro_);
    }

    mysql_res = mysql_store_result(m_conn_ro_);

    if (mysql_res == nullptr) {
      res = -1;
      if (detail_log) {
        std::cerr << "Failed to test consistency after insert, mysql res is "
                     "nullptr, sql: "
                  << query << std::endl;
      }
      break;
    }

    if (mysql_res->row_count == 0) {
      mysql_free_result(mysql_res);
      if (detail_log) {
        std::cerr << "Failed to test consistency after insert, mysql res "
                     "row_count is 0, sql: "
                  << query << std::endl;
      }
      break;
    }

    mysql_free_result(mysql_res);
  } while (0);

  commit_conn_trx(m_conn_ro_);
  return res;
}

int TestC::insert_test(uint64_t pk) {
  int res = 0;
  string query = "insert into " + m_table_name_ + " values(" +
                 std::to_string(pk) + "," + "0)";
  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cerr << "Failed to insert, sql: " << query
              << ", errno: " << mysql_errno(m_conn_rw_)
              << ", errmsg: " << mysql_error(m_conn_rw_) << std::endl;
  }

  return res;
}

int TestC::update(uint64_t &pk, uint64_t &old_value, uint64_t &new_value) {
  int res = 0;
  MYSQL_RES *mysql_res = nullptr;
  MYSQL_ROW row;
  string query;

  pk = rand() % m_table_size_ + 1;
  new_value = rand() % m_table_size_;

  query =
      "select c1 from " + m_table_name_ + " where id = " + std::to_string(pk);

  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cout << "Failed to test consistency, sql: " << query
              << ", errno: " << mysql_errno(m_conn_rw_)
              << ", errmsg: " << mysql_error(m_conn_rw_);
    return -1;
  }

  mysql_res = mysql_store_result(m_conn_rw_);

  row = mysql_fetch_row(mysql_res);
  if (row == nullptr) {
    if (detail_log) {
      std::cerr << "RW row is nullptr, expected: " << std::endl;
    }
    res = -1;
    return res;
  }

  old_value = strtoull(row[0], nullptr, 10);
  mysql_free_result(mysql_res);

  query = "update " + m_table_name_ + " set c1 = " + std::to_string(new_value) +
          " where id = " + std::to_string(pk);

  res = mysql_query(m_conn_rw_, query.data());
  if (res != 0) {
    std::cerr << "Failed to update, sql: " << query
              << ", errno: " << mysql_errno(m_conn_rw_)
              << ", error: " << mysql_error(m_conn_rw_);
  }
  return res;
}

int TestC::consistency_test(uint64_t pk, uint64_t old_value,
                            uint64_t expected) {
  int res = 0;
  MYSQL_RES *mysql_res = nullptr;
  MYSQL_ROW row;
  uint64_t ro_val;
  bool failed = false;
  string query =
      "select c1 from " + m_table_name_ + " where id = " + std::to_string(pk);
  do {
    res = mysql_query(m_conn_ro_, query.data());
    if (res != 0) {
      std::cerr << "Failed to test consistency, sql: " << query
                << ", errno: " << mysql_errno(m_conn_ro_)
                << ", errmsg: " << mysql_error(m_conn_ro_);
      break;
    }

    mysql_res = mysql_store_result(m_conn_ro_);

    row = mysql_fetch_row(mysql_res);
    if (row == nullptr) {
      if (detail_log) {
        std::cerr << "RO row is nullptr, expected: " << expected << std::endl;
      }
      res = -1;
      break;
    }
    ro_val = strtoull(row[0], nullptr, 10);
    mysql_free_result(mysql_res);
    mysql_res = nullptr;
    if (ro_val != expected) {
      if (detail_log) {
        std::cerr << "RO val: " << ro_val << ", expected: " << expected
                  << ", RW old: " << old_value << ", query: " << query
                  << std::endl;
      }
      failed = true;
      res = -1;
      if (sleep_after_sct_failed > 0) {
        sleep(sleep_after_sct_failed);
      }
    }

    if (failed) {
      res = -1;
    }

  } while (0);

  return res;
}

int TestC::run() {
  int res = 0;
  uint64_t pk = 0;
  uint64_t old_val = 0;
  uint64_t new_val = 0;
  if ((res = conns_prepare()) != 0) {
    return -1;
  }

  if (!skip_prepare){
    if (detail_log) {
      std::cout << "thread id: " << m_thread_id_ << " data preparing." << std::endl;
    }

    if ((res = data_prepare() != 0)) {
      return -1;
    }
  }

  if (short_connection) {
    conns_close();
  }

  running_threads++;

  while (processed_times++ < iterations) {
    if (short_connection) {
      conns_prepare();
    }
    state.increase_cnt_total();
    res = update(pk, old_val, new_val);
    if (res != 0) {
      conns_close();
      return -1;
    }

    if (sc_gap_us != 0) {
      usleep(sc_gap_us);
    }

    res = consistency_test(pk, old_val, new_val);
    if (res != 0) {
      state.increase_cnt_failed();
    }
    if (short_connection) {
      conns_close();
    }
  }

  running_threads--;

  if (detail_log) {
    std::cout << "thread id: " << m_thread_id_ << " finish." << std::endl;
  }
  return res;
}

int TestC::cleanup() {
  if (m_conn_rw_ != nullptr) {
    mysql_close(m_conn_rw_);
    m_conn_rw_ = nullptr;
  }

  if (m_conn_ro_ != nullptr) {
    mysql_close(m_conn_ro_);
    m_conn_ro_ = nullptr;
  }
  mysql_thread_end();
  return 0;
}

int main_sct() {
  std::thread *ct_threads[concurrency];
  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id] = new std::thread(start_test, thread_id);
    active_threads++;
  }

  Statistics new_state;
  Statistics pre_state;
  pre_state = state;

  if (report_interval != 0) {
    while (active_threads.load() != 0) {
      sleep(report_interval);

      if (running_threads > 0) {
        new_state = state;
        std::cout << "Strict consistency tps: "
                  << (new_state.get_cnt_total() - pre_state.get_cnt_total()) /
                         report_interval
                  << ", failed tps: "
                  << (new_state.get_cnt_failed() - pre_state.get_cnt_failed()) /
                         report_interval
                  << std::endl;
        pre_state = new_state;
      }
    }
  }

  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id]->join();
    delete ct_threads[thread_id];
  }

  std::cout << "Test strict consistency cnt: " << state.get_cnt_total()
            << ", failed cnt: " << state.get_cnt_failed() << std::endl;
  return 0;
}
