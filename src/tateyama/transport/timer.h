#include <cstdlib>
#include <thread>
#include <ctime>
#include <atomic>
#include <cstdint>
#include <functional>
#include <unistd.h>
#include <sys/timerfd.h>

namespace tateyama::common::wire {

class timer {
  public:
    timer(std::int64_t interval, const std::function<bool()>& work) : work_(work), tfd_(timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC)) {
        struct itimerspec its{};
        its.it_value.tv_sec = interval;
        its.it_value.tv_nsec = 0;
        its.it_interval.tv_sec = interval;
        its.it_interval.tv_nsec = 0;
        timerfd_settime(tfd_, 0, &its, nullptr);

        thread_ = std::thread(std::ref(*this));
    }
    ~timer() {
        stop_flag_ = true;

        struct itimerspec its{};
        its.it_value.tv_sec = 0;
        its.it_value.tv_nsec = 10L * 1000L * 1000L;
        its.it_interval.tv_sec = 0;
        its.it_interval.tv_nsec = 10L * 1000L * 1000L;
        timerfd_settime(tfd_, 0, &its, nullptr);

        if (thread_.joinable()) {
            thread_.join();
        }
        close(tfd_);
    }
    timer(timer const&) = delete;
    timer(timer&&) = delete;
    timer& operator = (timer const&) = delete;
    timer& operator = (timer&&) = delete;

    void operator()() {
        std::uint64_t t_cnt{};
        while (true) {
            auto r_size = read(tfd_, &t_cnt, sizeof(t_cnt));
            if (r_size != sizeof(t_cnt)) {
                break;
            }
            if(stop_flag_){
                break;
            }
            if (!work_()) {
                break;
            }
        }
    }
    
  private:
    const std::function<bool()>& work_;
    int tfd_;
    std::thread thread_{};
    std::atomic_bool stop_flag_{};
};

}
