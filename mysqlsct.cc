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

#define MYSQLSCT_VERSION "0.0.1"

using std::cout;
using std::endl;
using std::string;

bool prepare_done{false};

std::atomic<uint32_t> active_threads{0};
std::atomic<uint64_t> processed_times{0};

static char *user = nullptr;
static char *password = nullptr;
static char *database = nullptr;
std::string table_name_prefix = "sct";

static char *create_table_query = nullptr;
static char *host_ro = nullptr;
static char *host_rw = nullptr;
static uint port_rw = 0;
static uint port_ro = 0;
static uint sc_gap_us = 0;
static uint table_cnt = 1;
static uint64_t concurrency = 1;
static uint64_t table_size = 1000;
static uint64_t iterations = 100000;
static uint64_t report_interval = 1; // s
static uint detail_log = 0;
static uint64_t sleep_after_sct_failed = 0; // s
static uint select_after_insert = 0;
static uint short_connection = 0;
static char *test_mode_str = nullptr;
enum TestMode {
  SHORT_CONNECT,
  CONSISTENT,
};

TestMode test_mode{SHORT_CONNECT};

void parse_test_mode() {
  if (test_mode_str == nullptr) {
    test_mode = CONSISTENT;
  } else if (strcasecmp(test_mode_str, "SHORT_CONNECT") == 0) {
    test_mode = SHORT_CONNECT;
  } else if (strcasecmp(test_mode_str, "SHORT_CONNECT") == 0) {
    test_mode = CONSISTENT;
  }
}

void usage();

void free_option() {
  if (create_table_query != nullptr) {
    free(create_table_query);
    create_table_query = nullptr;
  }
  
  if (test_mode_str != nullptr) {
    free(test_mode_str);
    test_mode_str = nullptr;
  }

  if (host_rw != nullptr) {
    free(host_rw);
    host_rw = nullptr;
  }

  if (host_ro != nullptr) {
    free(host_ro);
    host_ro = nullptr;
  }

  if (user != nullptr) {
    free(user);
    user = nullptr;
  }

  if (password != nullptr) {
    free(password);
    password = nullptr;
  }

  if (database != nullptr) {
    free(database);
    database = nullptr;
  }
}

static const struct option long_options[] = {
    {"version", 0, nullptr, 'v'},          {"help", 0, nullptr, '?'},
    {"host-rw", 1, nullptr, 'h'},          {"host-ro", 1, nullptr, 'H'},
    {"database", 1, nullptr, 'D'},         {"port-rw", 1, nullptr, 'P'},
    {"port-ro", 1, nullptr, 'O'},          {"user", 1, nullptr, 'u'},
    {"password", 1, nullptr, 'p'},         {"iterations", 1, nullptr, 'i'},
    {"table-cnt", 1, nullptr, 'T'},        {"table-size", 1, nullptr, 't'},
    {"sc-gap-us", 1, nullptr, 's'},        {"report-interval", 1, nullptr, 'r'},
    {"detail-log", 1, nullptr, 'k'},       {"concurrency", 1, nullptr, 'c'},
    {"short-connection", 1, nullptr, 'S'}, {"test-after-insert", 1, nullptr, 'E'},
    {"create-table-query", 1, nullptr, 'A'}, {"test-mode", 1, nullptr, 'm'}
};

bool parse_option(int argc, char *argv[]) {
  int opt = 0;
  if (argc == 1) {
    usage();
    return false;
  }

  while ((opt = getopt_long(argc, argv, "?vH:h:D:P:O:i:T:t:s:r:u:p:c:k:S:E:m:A",
                            long_options, nullptr)) != -1) {
    switch (opt) {
      case 'm':
        test_mode_str = strdup(optarg);
        parse_test_mode();
        break;
    case 'A':
      create_table_query = strdup(optarg);
      break;

    case 'S':
      short_connection = atoi(optarg);
      break;

    case '?':
      usage();
      return false;
    case 'v':
      cout << MYSQLSCT_VERSION << endl;
      return false;

    case 'h':
      host_rw = strdup(optarg);
      break;

    case 'H':
      host_ro = strdup(optarg);
      break;

    case 'D':
      database = strdup(optarg);
      break;

    case 'P':
      port_rw = atoi(optarg);
      break;

    case 'O':
      port_ro = atoi(optarg);
      break;

    case 'i':
      iterations = atoll(optarg);
      break;

    case 'T':
      table_cnt = atoi(optarg);
      break;

    case 't':
      table_size = atoi(optarg);
      break;

    case 's':
      sc_gap_us = atoi(optarg);
      break;

    case 'r':
      report_interval = atoi(optarg);
      break;

    case 'u':
      user = strdup(optarg);
      break;

    case 'p':
      password = strdup(optarg);
      break;

    case 'c':
      concurrency = atoi(optarg);
      break;

    case 'k':
      detail_log = atoi(optarg);
      break;

    case 'E':
      select_after_insert = atoi(optarg);
      break;

    default:
      cout << "parse argument failed" << endl;
      usage();
      return false;
    }
  }

  return true;
}

void usage() {
  cout << "Usage: mysqlsct [OPTIONS]" << endl;
  cout << "-?	--help		Display this help and exit.\n";
  cout << "-v	--version	Output version information and exit.\n";
  cout << "-h	--host-rw	mysql RW node host.\n";
  cout << "-H	--host-ro	mysql RO node host.\n";
  cout << "-D	--database	mysql database.\n";
  cout << "-P	--port-rw	mysql RW node port.\n";
  cout << "-o	--port-ro	mysql RO node port.\n";
  cout << "-u	--user	mysql user.\n";
  cout << "-p	--password	mysql password.\n";
  cout << "-i	--iterations	number of times to run the tests.\n";
  cout << "-T	--table-cnt	number of tables to test.\n";
  cout << "-t	--table-size	table size.\n";
  cout << "-s	--sc-gap-us	time(us) to sleep after write.\n";
  cout << "-r	--report-interval	periodically report intermediate "
          "statistics with a specified interval in seconds.\n";
  cout << "-k	--detail-log	print detail error log.\n";
  cout << "-c	--concurrency	number of threads to use.\n";
  cout << "-S   --short-connection use short connection.\n";
  cout << "-E   --test-after-insert test RO after insert.\n";
}

bool verify_variables() {
  bool res = true;
  if (test_mode == SHORT_CONNECT) {

  } else {
    cout << "Input parameters: " << endl;
    cout << "host-rw: " << host_rw << endl;
    cout << "host-ro: " << host_ro << endl;
    cout << "database: " << database << endl;
    cout << "port-rw: " << port_rw << endl;
    cout << "port-ro: " << port_ro << endl;
    cout << "user: " << user << endl;
    cout << "password: " << password << endl;
    cout << "iterations: " << iterations << endl;
    cout << "table-cnt: " << table_cnt << endl;
    cout << "table-size: " << table_size << endl;
    cout << "sc-gap-us: " << sc_gap_us << endl;
    cout << "report-interval: " << report_interval << endl;
    cout << "detail-log: " << detail_log << endl;
    cout << "concurrency: " << concurrency << endl;
  }
  cout << "###########################################" << endl;
  if (host_rw == nullptr && test_mode != SHORT_CONNECT) {
    std::cerr << "miss host_rw.\n";
    res = false;
  }

  if (host_ro == nullptr && test_mode != SHORT_CONNECT) {
    std::cerr << "miss host_ro.\n";
    res = false;
  }

  if (database == nullptr) {
    std::cerr << "miss database.\n";
    res = false;
  }

  if (port_rw == 0) {
    std::cerr << "miss port_rw.\n";
    res = false;
  }

  return res;
}

MYSQL *safe_connect(const char *host, const char *user, const char *password,
                    const char *db, unsigned int port, char *errmesg) {
  MYSQL *conn;
  conn = mysql_init(0);
  unsigned int connection_timeout = 5;
  mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, (char *)&connection_timeout);
  if (!mysql_real_connect(conn, host, user, password, db, port, nullptr, 0)) {
    sprintf(errmesg, "connect to %s:%d error %d,%s", host, port,
            mysql_errno(conn), mysql_error(conn));
    return nullptr;
  }
  return conn;
}

int safe_close(MYSQL *conn, char *errmesg) {
  int ret = 0;
  if (!conn) {
    return ret;
  }
  mysql_free_result(mysql_store_result(conn));
  if (mysql_ping(conn) == 0) {
    mysql_close(conn);
  }
  return ret;
}

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
  main_sct();
  mysql_library_end();
  free_option();
  return ret;
}

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

private:
  std::atomic<uint64_t> m_cnt{0};
  std::atomic<uint64_t> m_cnt_failed{0};
};

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
  int test_short_connection();
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

int TestC::test_short_connection() {
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
    if (sc_gap_us != 0) {
      usleep(sc_gap_us);
    }

    if (mysql_query(m_conn_rw_, "select 1") != 0) {
      std::cerr << "Failed to execut query: select 1."
                << " errno: " << mysql_errno(m_conn_rw_)
                << ",errmsg: " << mysql_error(m_conn_rw_) << std::endl;
      res = -1;
    }

    if (m_conn_rw_ != nullptr) {
      mysql_close(m_conn_rw_);
      m_conn_rw_ = nullptr;
    }
  } while (0);

  return res;
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
    query = "/* mysqlsct test after insert */ select * from " + m_table_name_ +
            " where id = " + std::to_string(pk);
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

  // commit_conn_trx(m_conn_ro_);
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
      "/* mysqlsct test after update */ select c1 from " + m_table_name_ + " where id = " + std::to_string(pk);
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
  if (test_mode != SHORT_CONNECT) {
    if ((res = conns_prepare()) != 0) {
      return -1;
    }
  }

  if (create_table_query != nullptr) {
    if ((res = data_prepare() != 0)) {
      return -1;
    }
  }

  if (short_connection) {
    conns_close();
  }

  uint64_t pk = 0;
  uint64_t old_val = 0;
  uint64_t new_val = 0;


  while (processed_times++ < iterations) {
    if (test_mode == SHORT_CONNECT) {
      if (test_short_connection() == 0) {
        state.increase_cnt_total();
      } else {
        state.increase_cnt_failed();
      }
      continue;
    }
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

void print_result_interval() {
  Statistics new_state;
  Statistics pre_state;
  pre_state = state;
  if (report_interval != 0) {
    // waiting for thread prepare completed.
    
    while (active_threads.load() != 0) {
      sleep(report_interval);
      new_state = state;
      if (test_mode == SHORT_CONNECT) {
      std::cout << "cd(connect/disconnect)ps: "
                << (new_state.get_cnt_total() - pre_state.get_cnt_total()) /
                       report_interval
                << ", failed cdps: "
                << (new_state.get_cnt_failed() - pre_state.get_cnt_failed()) /
                       report_interval
                << std::endl;
        
      } else {
      std::cout << "Strict consistency tps: "
                << (new_state.get_cnt_total() - pre_state.get_cnt_total()) /
                       report_interval
                << ", failed tps: "
                << (new_state.get_cnt_failed() - pre_state.get_cnt_failed()) /
                       report_interval
                << std::endl;

      }
      pre_state = new_state;
    }
  }
}

void print_result_summarize() {
  if (test_mode == SHORT_CONNECT) {
    std::cout << "Test connection/disconnect cnt: " << state.get_cnt_total()
              << ", failed cnt: " << state.get_cnt_failed() << std::endl;
  } else {
    std::cout << "Test strict consistency cnt: " << state.get_cnt_total()
              << ", failed cnt: " << state.get_cnt_failed() << std::endl;
  }
}

int main_sct() {
  std::thread *ct_threads[concurrency];
  for (uint thread_id = 0; thread_id < concurrency; thread_id++) {
    ct_threads[thread_id] = new std::thread(start_test, thread_id);
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
