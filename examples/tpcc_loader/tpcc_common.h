/*
 * Copyright 2019-2019 tsurugi project.
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

#ifndef TPCC_COMMON_H_
#define TPCC_COMMON_H_

#include <cstring>
#include <random>
#include <ctime>
#include <array>
#include <chrono>
#include <iomanip>

#include "ogawayama/stub/api.h"

namespace ogawayama {
namespace tpcc {

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

}  // namespace tpcc
}  // namespace ogawayama

#endif  // TPCC_COMMON_H_
