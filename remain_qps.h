#ifndef REMAIN_QPS_H
#define REMAIN_QPS_H

#include "short_connection.h"

class RemainQPSTest : public ShortConnnectionTest {
 public:
   RemainQPSTest(const char *db_name, int64_t times, int64_t qps)
       : ShortConnnectionTest(db_name, times) {
     test_qps = qps;
   }
   virtual void run(const std::vector<std::string> &querys);
   virtual int basic_query(const std::vector<std::string> &querys);
   int test_qps;
};

int main_remain_qps();

#endif // REMIAN_QPS_H
