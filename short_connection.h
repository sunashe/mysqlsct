/*
 * @Date         : 2023-03-29 15:21:21
 * @Author       : huyutuo.hyt
 * @FilePath     : short_connection.h
 * @Description  :
 */

#ifndef SHORT_CONNECTION_H
#define SHORT_CONNECTION_H
#include <cstdint>
#include <mysql/mysql.h>
#include <string>
#include <vector>

class ShortConnnectionTest {
public:
  ShortConnnectionTest(const char *db_name, int64_t times) {
    m_db_name_ = db_name;
    m_times_ = times;
  }

  virtual void run(const std::vector<std::string> &querys);
  int cleanup();
  virtual int basic_query(const std::vector<std::string> &querys);

protected:
  int conns_prepare();
  void conns_close();

  MYSQL *m_conn_{nullptr};
  const char *m_db_name_;
  uint64_t m_times_;
};

// get querys from shot_connection_querys.txt
std::vector<std::string> get_querys_from_file();

void start_short_connection_test(int thread_id,
                                 const std::vector<std::string> &querys);

int main_shortct();

#endif // SHORT_CONNECTION_H
