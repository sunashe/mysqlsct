/*
 * @Date         : 2023-03-29 15:47:02
 * @Author       : huyutuo.hyt
 * @FilePath: options.cc
 * @Description  : 
 */


#include <cstddef>
#include <cstring>
#include <getopt.h>
#include <ostream>
#include <string>
#include <iostream>

#include "options.h"

using std::cout;
using std::endl;

#define MYSQLSCT_VERSION "0.0.1"

char *user = nullptr;
char *password = nullptr;
char *database = nullptr;
std::string table_name_prefix = "sct";
std::string secondary_index_prefix = "sec_index";

char *host_ro = nullptr;
char *host_rw = nullptr;
char *host_rw2 = nullptr;
uint port_rw2 = 0;
char *host = nullptr;
uint port = 0;
uint port_rw = 0;
uint port_ro = 0;
uint sc_gap_us = 0;
uint table_cnt = 1;
uint64_t concurrency = 1;
uint64_t table_size = 1000;
uint64_t iterations = 100000;
uint64_t report_interval = 1; // s
uint detail_log = 0;
uint64_t sleep_after_sct_failed = 0; // s
uint select_after_insert = 0;
uint short_connection = 0;
bool skip_prepare = 0;
uint64_t test_time = 60;
uint64_t test_qps = 1000;

// test_mode contains "sct", "shortct", "rqps"
char *test_mode_str = nullptr;

TestMode test_mode{CONSISTENT}; // defalut sct mode

void parse_test_mode() {
  if (test_mode_str == nullptr || strcasecmp(test_mode_str, "sct") == 0) {
    test_mode = CONSISTENT;
  } else if (strcasecmp(test_mode_str, "shortct") == 0) {
    test_mode = SHORT_CONNECT;
  } else if (strcasecmp(test_mode_str, "rqps") == 0) {
    test_mode = REMAIN_QPS;
  }
}

int flag = 0;
static const struct option long_options[] = {
    {"version", 0, nullptr, 'v'},          {"help", 0, nullptr, '?'},
    {"host-rw", 1, nullptr, 'h'},          {"host-ro", 1, nullptr, 'H'},
    {"database", 1, nullptr, 'D'},         {"port-rw", 1, nullptr, 'P'},
    {"port-ro", 1, nullptr, 'O'},          {"user", 1, nullptr, 'u'},
    {"password", 1, nullptr, 'p'},         {"iterations", 1, nullptr, 'i'},
    {"table-cnt", 1, nullptr, 'T'},        {"table-size", 1, nullptr, 't'},
    {"sc-gap-us", 1, nullptr, 's'},        {"report-interval", 1, nullptr, 'r'},
    {"detail-log", 1, nullptr, 'k'},       {"concurrency", 1, nullptr, 'c'},
    {"short-connection", 1, nullptr, 'S'}, {"test-mode", 1, nullptr, 'm'},
    {"port", 1, nullptr, 'R'}, {"host", 1, nullptr, 'o'},
    {"skip-prepare", 1, nullptr, 'K'},     {"sleep-after-fail", 1, nullptr, 'f'},
    {"port", 1, nullptr, 'R'},             {"host", 1, nullptr, 'o'},
    {"test-time", 1, &flag, 1},            {"qps", 1, &flag, 2},
    {"host-rw2", 1, &flag, 3},             {"port-rw2", 1, &flag, 4},
    {nullptr, 0, nullptr, 0}
};


bool parse_option(int argc, char *argv[]) {
  int opt = 0;
  if (argc == 1) {
    usage();
    return false;
  }

  while (
      (opt = getopt_long(argc, argv, "?vH:h:D:P:O:i:T:t:s:r:u:p:c:k:S:m:R:o:K:f:",
                         long_options, nullptr)) != -1) {
    switch (opt) {
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
    
    case 'm':
      test_mode_str = strdup(optarg);
      parse_test_mode();
      break;
    
    case 'R':
      port = atoi(optarg);
      break;
    
    case 'o':
      host = strdup(optarg);
      break;

    case 'K':
      skip_prepare = atoi(optarg) > 0 ? true : false;
	  break;

    case 'f':
      sleep_after_sct_failed = atoi(optarg);
	  break;
    case 0: {
      switch (flag) {
        case 1 :
          test_time = atoi(optarg);
          break;
        case 2 :
          test_qps = atoi(optarg);
          break;
        case 3:
          host_rw2 = strdup(optarg);
          break;
        case 4:
          port_rw2 = atoi(optarg);
          break;
      }
      break;
    }

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
  cout << "-S	--short-connection use short connection.\n";
  cout << "-m	--test-mode choose test mode.\n";
  cout << "-K	--skip-prepare skip data prepare.\n";
  cout << "-f	--sleep-after-fail sleep ms after sct failed";
  cout << "--test-time the totol time in remain_qps mode\n";
  cout << "--qps the qps you want to remain in test\n";
}

bool verify_variables() {
  bool res = true;
  cout << "Input parameters: " << endl;
  if (host_rw)
    cout << "host-rw: " << host_rw << endl;
  if (host_ro)
    cout << "host-ro: " << host_ro << endl;
  if (host)
    cout << "host" << host << endl;
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
  cout << "short-connection: " << short_connection << endl;
  cout << "test-mode: " << test_mode << endl;
  cout << "skip-prepasre: " << skip_prepare << endl;
  cout << "sleep-after-fail: " << sleep_after_sct_failed << endl;
  cout << "test-time: " << test_time << endl;
  cout << "test-qps: " << test_qps << endl;
  cout << "###########################################" << endl;

  if (test_mode == TestMode::CONSISTENT) {
    if (host_rw == nullptr) {
      std::cerr << "miss host_rw.\n";
      res = false;
    }

    if (host_ro == nullptr) {
      std::cerr << "miss host_ro.\n";
      res = false;
    }

    if (port_rw == 0) {
      std::cerr << "miss port_rw.\n";
      res = false;
    }
  } else if (test_mode == TestMode::SHORT_CONNECT ||
             test_mode == TestMode::REMAIN_QPS) {
    if (host == nullptr) {
      std::cerr << "miss host \n";
      res = false;
    }
    if (port == 0) {
      std::cerr << "miss port.\n";
      res = false;
    }
  } else {
    std::cerr << "wrong test mode: " << test_mode << endl;
    std::cerr << "only suport sct, shotct mode now" << endl;
    res = false;
  }

  if (database == nullptr) {
    std::cerr << "miss database.\n";
    res = false;
  }

  return res;
}

void free_option() {
  if (test_mode_str != nullptr) {
    free(test_mode_str);
    test_mode_str = nullptr;
  }

  if (host != nullptr) {
    free(host);
    host = nullptr;
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
