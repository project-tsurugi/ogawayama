#pragma once

#include <cstdint>

namespace ogawayama {
namespace tpcc {

enum class Profile {
    TINY_SCALE = 0,
    DEFAULT_SCALE = 1,
};

template<Profile>
struct Scale {};

template<>
struct Scale<Profile::TINY_SCALE> {
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

template<>
struct Scale<Profile::DEFAULT_SCALE> {
    /** Number of warehouses. Does not grow dynamically */
    static constexpr inline std::uint16_t warehouses = 1U;

    /** Number of items per warehouse. Does not grow dynamically  */
    static constexpr inline std::uint32_t items = 100000U;

    /** Number of districts per warehouse. Does not grow dynamically  */
    static constexpr inline std::uint8_t districts = 10U;

    /** Number of customers per district. Does not grow dynamically  */
    static constexpr inline std::uint32_t customers = 3000U;

    /** Number of orders per district. Does grow dynamically. */
    static constexpr inline std::uint32_t orders = 3000U;

    /** Number of orderlines per order. Does not grow dynamically. */
    static constexpr inline std::uint16_t max_ol_count = 15U;
    static constexpr inline std::uint8_t min_ol_count = 5U;
    static constexpr inline std::uint16_t max_ol = max_ol_count + 1U;

    /** Number of variations of last names. Does not grow dynamically. */
    static constexpr inline std::uint32_t lnames = 1000U;

    static constexpr inline std::int32_t load_threads = 5 + (districts / 2);
};

#if !defined(TPCC_SCALE)
#define TPCC_SCALE Profile::DEFAULT_SCALE
#endif  // TPCC_SCALE

using scale = Scale<static_cast<Profile>(TPCC_SCALE)>;

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
