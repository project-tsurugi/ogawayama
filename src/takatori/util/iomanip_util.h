#pragma once

#include <iomanip>
#include <ostream>

namespace takatori::util {

class instant_fill {
public:
    explicit instant_fill(std::ostream& out, char fill)
        : out_(out)
        , last_fill_(out.fill())
    {
        out_ << std::setfill(fill);
    }

    ~instant_fill() {
        out_ << std::setfill(last_fill_);
    }

    instant_fill(instant_fill const&) = delete;
    instant_fill& operator=(instant_fill const&) = delete;
    instant_fill(instant_fill&&) noexcept = delete;
    instant_fill& operator=(instant_fill&&) noexcept = delete;

private:
    std::ostream& out_;
    char last_fill_;
};

class instant_showpos {
public:
    explicit instant_showpos(std::ostream& out)
            : out_(out)
            , last_showpos_((out.flags() & std::ostream::showpos) != 0)
    {
        out_ << std::showpos;
    }

    ~instant_showpos() {
        if (!last_showpos_) out_ << std::noshowpos;
    }

    instant_showpos(instant_showpos const&) = delete;
    instant_showpos& operator=(instant_showpos const&) = delete;
    instant_showpos(instant_showpos&&) noexcept = delete;
    instant_showpos& operator=(instant_showpos&&) noexcept = delete;

private:
    std::ostream& out_;
    bool last_showpos_;
};

} // namespace takatori::util

