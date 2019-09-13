#ifndef TPCC_SCALE_H_
#define TPCC_SCALE_H_

#include <cstdint>

namespace ogawayama {
namespace tpcc {

struct Scale {
    /** Number of warehouses. Does not grow dynamically */
    static constexpr inline std::uint16_t warehouses = 1;

    /** Number of items per warehouse. Does not grow dynamically  */
    static constexpr inline std::uint32_t items = 50;

    /** Number of districts per warehouse. Does not grow dynamically  */
    static constexpr inline std::uint8_t districts = 2;

    /** Number of customers per district. Does not grow dynamically  */
    static constexpr inline std::uint32_t customers = 30;

    /** Number of orders per district. Does grow dynamically. */
    static constexpr inline std::uint32_t orders = 30;

    /** Number of orderlines per order. Does not grow dynamically. */
    static constexpr inline std::uint16_t max_ol_count = 6;
    static constexpr inline std::uint8_t min_ol_count = 2;
    static constexpr inline std::uint16_t max_ol = max_ol_count + 1U;

    /** Number of variations of last names. Does not grow dynamically. */
    static constexpr inline std::uint32_t lnames = 1000U;

    static constexpr inline std::int32_t load_threads = 1;
};

using scale = Scale;

extern uint16_t warehouses;

// See Sec 5.2.2 of the TPCC spec
constexpr inline std::uint8_t kXctNewOrderPercent = 45U;
constexpr inline std::uint8_t kXctPaymentPercent = 43U + kXctNewOrderPercent;
constexpr inline std::uint8_t kXctOrderStatusPercent = 4U + kXctPaymentPercent;
constexpr inline std::uint8_t kXctDelieveryPercent = 4U + kXctOrderStatusPercent;
// remainings are stock-level xct.

/**
 * How much of supplier in neworder transaction uses remote warehouse?
 * Note that one neworder xct touches on average 10 suppliers, so in total
 * a bit less than 10% of neworder accesses remote warehouse(s).
 */
constexpr inline std::uint8_t kNewOrderRemotePercent = 1U;

/** How much of payment transaction uses remote warehouse/district? */
// const uint8_t kPaymentRemotePercent = 15U;

}  // namespace tpcc
}  // namespace ogawayama

#endif  // TPCC_SCALE_H_
