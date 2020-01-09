/*
 * Copyright 2018-2020 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstring>
#include <random>
#include <ctime>
#include <array>
#include <chrono>
#include <iomanip>

#include "ogawayama/stub/api.h"

namespace ogawayama {
namespace tpcc {

using Database = std::remove_pointer<ConnectionPtr::element_type>::type;
struct cached_statement
{
    const char *sql;		/* statement text */
    PreparedStatementPtr prepared{};
};

[[maybe_unused]] 
static void plan_queries(cached_statement *statements, ConnectionPtr::element_type *connection)
{
    cached_statement *s;

    if(!statements->prepared) {
	/* On first invocation, plan all the queries */
	for (s = statements; s->sql != NULL; s++)
	{
            
            if (connection->prepare( s->sql, s->prepared) != ERROR_CODE::OK) {
                LOG(WARNING) << "failed to prepare statement: " << s->sql;
            }
	}
    }
}

#define elog(...)
#define PG_RETURN_INT32(v) return v

using int32 = std::int32_t;
using float4 = float;
 
  // closer
  template <typename T>
    class Closer {
  public:
    explicit Closer(T& t) : t_(t) {}
    Closer() = default;
    Closer(const Closer& other) = default;
    Closer(Closer&& other) = default;
    Closer& operator=(const Closer& other) = default;
    Closer& operator=(Closer&& other) = default;
    ~Closer() {
      t_.close();
    }
  private:
    T& t_;
  };

  struct scope_guard { // NOLINT(hicpp-special-member-functions, cppcoreguidelines-special-member-functions)
      explicit scope_guard(std::function<void(void)> finally) : finally_(std::move(finally)) {}
      ~scope_guard() {
          finally_();
      }
      std::function<void(void)> finally_{};
  };

  // exceptions
  class SQL_ROW_NOT_FOUND : public std::exception {};
  class SQL_ROW_MULTIPLE_HIT : public std::exception {};

  // random
  class randomGeneratorClass {
  private:
    std::mt19937 mt;
  
  public:
    randomGeneratorClass() {
      std::random_device rnd;
      mt.seed(rnd());
    }
    unsigned int uniformWithin(unsigned int low, unsigned int high)
    {
      std::uniform_int_distribution<> randlh(low, high);
      return randlh(mt);
    }
    unsigned int nonUniformWithin(unsigned int A, unsigned int x, unsigned int y)
    {
      unsigned int C = uniformWithin(0, A);
      return (((uniformWithin(0, A) | uniformWithin(x, y)) + C) % (y - x + 1)) + x;
    }
    void MakeAddress(char *str1, char *str2, char *city, char *state, char *zip)
    {
      MakeAlphaString(10,20,str1); /* Street 1*/
      MakeAlphaString(10,20,str2); /* Street 2*/
      MakeAlphaString(10,20,city); /* City */
      MakeAlphaString(2,2,state); /* State */
      MakeNumberString(9,9,zip); /* Zip */
    }
    int MakeAlphaString(int min, int max, char *str)
    {
      const char character = 'a';
      std::string ss;

      int length = uniformWithin(min, max);
      for (int i = 0; i < length;  ++i) {
        ss += static_cast<char>(character + uniformWithin(0, 25));
      }
      strncpy(static_cast<char *>(str), ss.data(), length);
      *(str+length) = '\0';			// NOLINT
      return length;
    }
    int MakeNumberString(int min, int max, char *str)
    {
      const char character = '0';
      std::string ss;
      
      int length = uniformWithin(min, max);
      for (int i = 0; i < length; ++i) {
        ss += static_cast<char>(character + uniformWithin(0, 9));
      }
      strncpy(static_cast<char *>(str), ss.data(), length);
      *(str+length) = '\0';			// NOLINT
      return length;
    }
    inline int RandomNumber(int low, int high) {
      return uniformWithin(low, high); }
  };
  
using TIMESTAMP = char[26];
using DATESTAMP = char[11];

static inline std::string
get_timestamp(int deltam=0)
{
    time_t now = time(nullptr);
    now -= deltam * 60L;
    TIMESTAMP ts;
    ctime_r(&now, ts);
    char *ptr = strstr(ts, "\n");
    if (ptr != nullptr) *ptr = '\0';
    return std::string(ts);
}

static inline
void get_datestamp(char *buf, int deltad=0)
{
    time_t now = time(nullptr);
    now -= deltad * (24L * 60L * 60L);
    struct tm *timeptr = localtime(&now);;
    strftime(buf,12,"%Y-%m-%d",timeptr);
}

  class ExceptionTpcc : public std::exception {
  public:
      explicit ExceptionTpcc(std::string_view msg) : msg_(msg) {}
  public:
      const char* what() const noexcept {
          return msg_.c_str();
      }
  private:
      std::string msg_;
  };

  // for data generation
  static inline void
  gettimestamp(char *buf, int deltam=0)
  {
    time_t now = time(nullptr);
    now -= deltam * 60L;
    ctime_r(&now, buf);
    char *ptr = strstr(buf, "\n");
    if (ptr != nullptr) *ptr = '\0';
  }

  static inline
  void getdatestamp(char *buf, int deltad=0)
  {
    time_t now = time(nullptr);
    now -= deltad * (24L * 60L * 60L);
    struct tm *timeptr = localtime(&now);;
    strftime(buf,12,"%Y-%m-%d",timeptr);
  }

  static inline
  void Lastname(int num, char *name)
  {
    const static std::array<std::string,10> n =
      {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
       "ESE", "ANTI", "CALLY", "ATION", "EING"};
    strcpy(name,(n.at(num/100)).c_str());
    strcat(name,(n.at((num/10)%10)).c_str());
    strcat(name,(n.at(num%10)).c_str());
  }

  static inline
  std::string double_to_string(double v, int n=2)
  {
    double d = 0.1;
    for(int i = 1; i < n; i++) d *= 0.1;
    d /= 2.0;

    std::string str = std::to_string(v+d);
    std::string::size_type dotp = str.find('.');
    return dotp != std::string::npos ? str.substr(0, dotp+1+n) : str;
  }

  void dmy_use(const char *pp[]);

  /*
   * Profiler
   */
class tpcc_profiler {
  public:
      tpcc_profiler() = default;
      const std::chrono::steady_clock::time_point NOT_DEFINED {};
      void print() {
          std::cout.imbue(std::locale(""));
          std::cout << "begin to execute:\t" << std::setw(16) << enter_time_.count() << std::endl;
          std::cout << "neworder:\t" << std::setw(16) << execute_time_neworder_.count() << "\t" << std::setw(14) << execute_count_neworder_ << std::endl;
          std::cout << "payment:\t" << std::setw(16) << execute_time_payment_.count() << "\t" << std::setw(14) << execute_count_payment_ << std::endl;
          std::cout << "orderstatus:\t" << std::setw(16) << execute_time_orderstatus_.count() << "\t" << std::setw(14) << execute_count_orderstatus_ << std::endl;
          std::cout << "delivery:\t" << std::setw(16) << execute_time_delivery_.count() << "\t" << std::setw(14) << execute_count_delivery_ << std::endl;
          std::cout << "stocklevel:\t" << std::setw(16) << execute_time_stocklevel_.count() << "\t" << std::setw(14) << execute_count_stocklevel_ << std::endl;
          std::cout << "execute to end:\t" << std::setw(16) << exit_time_.count() << std::endl;
          std::cout.imbue(std::locale::classic());
      }
      tpcc_profiler& operator+=(tpcc_profiler &rhs) {
          enter_time_ += rhs.enter_time_;
          execute_time_neworder_ += rhs.execute_time_neworder_;
          execute_time_payment_ += rhs.execute_time_payment_;
          execute_time_orderstatus_ += rhs.execute_time_orderstatus_;
          execute_time_delivery_ += rhs.execute_time_delivery_;
          execute_time_stocklevel_ += rhs.execute_time_stocklevel_;
          execute_count_neworder_ += rhs.execute_count_neworder_;
          execute_count_payment_ += rhs.execute_count_payment_;
          execute_count_orderstatus_ += rhs.execute_count_orderstatus_;
          execute_count_delivery_ += rhs.execute_count_delivery_;
          execute_count_stocklevel_ += rhs.execute_count_stocklevel_;
          exit_time_ += rhs.exit_time_;
          return *this;
      }
      void mark_enter() { enter_time_mark_ = std::chrono::steady_clock::now(); }
      void mark_neworder_begin() { if ( begin_time_neworder_mark_ == NOT_DEFINED ) begin_time_neworder_mark_ = std::chrono::steady_clock::now(); }
      void mark_payment_begin() { if ( begin_time_payment_mark_ == NOT_DEFINED ) begin_time_payment_mark_ = std::chrono::steady_clock::now(); }
      void mark_orderstatus_begin() { if ( begin_time_orderstatus_mark_ == NOT_DEFINED ) begin_time_orderstatus_mark_ = std::chrono::steady_clock::now(); }
      void mark_delivery_begin() { if ( begin_time_delivery_mark_ == NOT_DEFINED ) begin_time_delivery_mark_ = std::chrono::steady_clock::now(); }
      void mark_stocklevel_begin() { if ( begin_time_stocklevel_mark_ == NOT_DEFINED ) begin_time_stocklevel_mark_ = std::chrono::steady_clock::now(); }
      void mark_neworder_end() { end_time_neworder_mark_ = std::chrono::steady_clock::now(); execute_count_neworder_++; }
      void mark_payment_end() { end_time_payment_mark_ = std::chrono::steady_clock::now(); execute_count_payment_++; }
      void mark_orderstatus_end() { end_time_orderstatus_mark_ = std::chrono::steady_clock::now(); execute_count_orderstatus_++; }
      void mark_delivery_end() { end_time_delivery_mark_ = std::chrono::steady_clock::now(); execute_count_delivery_++; }
      void mark_stocklevel_end() { end_time_stocklevel_mark_ = std::chrono::steady_clock::now(); execute_count_stocklevel_++; }
      void mark_exit() {
          std::chrono::steady_clock::time_point exit_time = std::chrono::steady_clock::now();
          std::chrono::steady_clock::time_point tx_begin_time{}, tx_end_time{};
          if ( end_time_neworder_mark_ != NOT_DEFINED ) {
              tx_begin_time = begin_time_neworder_mark_;
              tx_end_time = end_time_neworder_mark_;
          } else if ( end_time_delivery_mark_ != NOT_DEFINED ) {
              tx_begin_time = begin_time_delivery_mark_;
              tx_end_time = end_time_delivery_mark_;
          } else if ( end_time_stocklevel_mark_ != NOT_DEFINED ) {
              tx_begin_time = begin_time_stocklevel_mark_;
              tx_end_time = end_time_stocklevel_mark_;
          } else if ( end_time_orderstatus_mark_ != NOT_DEFINED ) {
              tx_begin_time = begin_time_orderstatus_mark_;
              tx_end_time = end_time_orderstatus_mark_;
          } else if ( end_time_payment_mark_ != NOT_DEFINED ) {
              tx_begin_time = begin_time_payment_mark_;
              tx_end_time = end_time_payment_mark_;
          } else {
              std::abort();
          }
          enter_time_ += std::chrono::duration_cast<std::chrono::microseconds>(tx_begin_time - enter_time_mark_);
          execute_time_neworder_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time_neworder_mark_ - begin_time_neworder_mark_);
          execute_time_delivery_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time_delivery_mark_ - begin_time_delivery_mark_);
          execute_time_stocklevel_+= std::chrono::duration_cast<std::chrono::microseconds>(end_time_stocklevel_mark_- begin_time_stocklevel_mark_);
          execute_time_orderstatus_+= std::chrono::duration_cast<std::chrono::microseconds>(end_time_orderstatus_mark_- begin_time_orderstatus_mark_);
          execute_time_payment_+= std::chrono::duration_cast<std::chrono::microseconds>(end_time_payment_mark_- begin_time_payment_mark_);
          exit_time_ += std::chrono::duration_cast<std::chrono::microseconds>(exit_time - tx_end_time);
          begin_time_neworder_mark_ = end_time_neworder_mark_ = NOT_DEFINED;
          begin_time_delivery_mark_ = end_time_delivery_mark_ = NOT_DEFINED;
          begin_time_stocklevel_mark_ = end_time_stocklevel_mark_ = NOT_DEFINED;
          begin_time_orderstatus_mark_ = end_time_orderstatus_mark_ = NOT_DEFINED;
          begin_time_payment_mark_ = end_time_payment_mark_ = NOT_DEFINED;
      }
  private:
      std::chrono::steady_clock::time_point enter_time_mark_{};
      std::chrono::steady_clock::time_point begin_time_neworder_mark_{};
      std::chrono::steady_clock::time_point begin_time_payment_mark_{};
      std::chrono::steady_clock::time_point begin_time_orderstatus_mark_{};
      std::chrono::steady_clock::time_point begin_time_delivery_mark_{};
      std::chrono::steady_clock::time_point begin_time_stocklevel_mark_{};
      std::chrono::steady_clock::time_point end_time_neworder_mark_{};
      std::chrono::steady_clock::time_point end_time_payment_mark_{};
      std::chrono::steady_clock::time_point end_time_orderstatus_mark_{};
      std::chrono::steady_clock::time_point end_time_delivery_mark_{};
      std::chrono::steady_clock::time_point end_time_stocklevel_mark_{};
      
      std::chrono::microseconds enter_time_{};
      std::chrono::microseconds execute_time_neworder_{};
      std::chrono::microseconds execute_time_payment_{};
      std::chrono::microseconds execute_time_orderstatus_{};
      std::chrono::microseconds execute_time_delivery_{};
      std::chrono::microseconds execute_time_stocklevel_{};
      std::chrono::microseconds exit_time_{};
      std::uint64_t execute_count_neworder_{};
      std::uint64_t execute_count_payment_{};
      std::uint64_t execute_count_orderstatus_{};
      std::uint64_t execute_count_delivery_{};
      std::uint64_t execute_count_stocklevel_{};
  };

}  // namespace tpcc
}  // namespace ogawayama
