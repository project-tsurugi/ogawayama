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
#ifndef SHARED_MEMORY_H_
#define SHARED_MEMORY_H_

#include <iostream>
#include <string.h>

#include <vector>
#include <variant>
#include <string_view>
#include "boost/interprocess/managed_shared_memory.hpp"
#include "boost/interprocess/allocators/allocator.hpp"
#include "boost/interprocess/containers/string.hpp"
#include "boost/thread/thread_time.hpp"

namespace ogawayama::common {

namespace param {
    static constexpr std::size_t SEGMENT_SIZE_SC = 1<<12;   // 4 KB (for server channel stream, tantative)
    static constexpr std::size_t SEGMENT_SIZE_CC = 1<<17;   // 128 KB (for client channel stream, tantative)
    static constexpr std::size_t SEGMENT_SIZE_RQ = 1<<20;   // 1 MB (for row queue, tantative)
    static constexpr std::size_t MAX_NAME_LENGTH = 64;   // 64 chars (tantative, but probably enough)
    static constexpr std::size_t QUEUE_SIZE = 32;        // 32 rows (tantative) must be greater than or equal to 2
    static constexpr long TIMEOUT = 10000;   	     // timeout for condition, currentry set to 10 seconds

    static constexpr char const * server = "server";
    static constexpr char const * connection = "connection";
    static constexpr char const * channel = "channel";
    static constexpr char const * resultset = "resultset";
    static constexpr char const * prepared = "prepared";

    /** @brief Represents the type of Shared memory.
     */
    enum SheredMemoryType {
        /**
         * @brief no meaning, used as a mark.
         */
        SHARED_MEMORY_NONE,
        /**
         * @brief shared memory for server channnel.
         */
        SHARED_MEMORY_SERVER_CHANNEL,
        /**
         * @brief shared memory for client channel.
         */
        SHARED_MEMORY_CONNECTION,
        /**
         * @brief shared memory for row queue.
         */
        SHARED_MEMORY_ROW_QUEUE,
    };
};  // namespace ogawayama::common::param


using CharAllocator = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;
using ShmString = boost::interprocess::basic_string<char, std::char_traits<char>, CharAllocator>;
using ShmClmArg = std::variant<std::monostate, std::int16_t, std::int32_t, std::int64_t, float, double, ShmString>;
using ShmClmArgAllocator = boost::interprocess::allocator<ShmClmArg, boost::interprocess::managed_shared_memory::segment_manager>;
using ShmRowArgs = std::vector<ShmClmArg, ShmClmArgAllocator>;
using ShmRowArgsAllocator = boost::interprocess::allocator<ShmRowArgs, boost::interprocess::managed_shared_memory::segment_manager>;

/**
 * @brief exception class for shared memory lost
 */
class SharedMemoryException : public std::exception {
public:
    SharedMemoryException() = default;
    SharedMemoryException( const std::string &str ) : m_error(str){}
    ~SharedMemoryException() override = default;
    SharedMemoryException(const SharedMemoryException& other) = delete;
    SharedMemoryException(SharedMemoryException&& other) = default;
    SharedMemoryException& operator=(const SharedMemoryException& other) = delete;
    SharedMemoryException& operator=(SharedMemoryException&& other) = default;
    const char* what( void ) const noexcept override { return m_error.c_str(); }

private:
    std::string m_error;
};

/**
 * @brief Shared memory manager in charge of creation and destruction of a boost managed shared memory
 */
class SharedMemory
{
public:
    /**
     * @brief Construct a new object.
     */
    SharedMemory(std::string_view database_name, param::SheredMemoryType type, bool create_shm, bool remove_shm = true) : database_name_(database_name), owner_(create_shm)  {
        if (create_shm) {
            std::size_t shm_size;
            switch(type) {
            case param::SheredMemoryType::SHARED_MEMORY_SERVER_CHANNEL: shm_size = param::SEGMENT_SIZE_SC; break;
            case param::SheredMemoryType::SHARED_MEMORY_CONNECTION: shm_size = param::SEGMENT_SIZE_CC; break;
            case param::SheredMemoryType::SHARED_MEMORY_ROW_QUEUE: shm_size = param::SEGMENT_SIZE_RQ; break;
            default: throw SharedMemoryException("invalid shared memory type");
            }
            if (remove_shm) {
                boost::interprocess::shared_memory_object::remove(database_name_.c_str());
            }
            try {
                managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, database_name_.c_str(), shm_size);
            }
            catch(const boost::interprocess::interprocess_exception& ex) {
                throw SharedMemoryException("shared memory already exist");
            }
        } else {
            try {
                managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, database_name_.c_str());
            }
            catch(const boost::interprocess::interprocess_exception& ex) {
                throw SharedMemoryException("can't find shared memory");
            }
        }
    }
    SharedMemory(std::string_view database_name) : SharedMemory(database_name, param::SheredMemoryType::SHARED_MEMORY_NONE, false) {}
    /**
     * @brief Destruct this object.
     */
    ~SharedMemory()
    {
        if (owner_) {
            boost::interprocess::shared_memory_object::remove(database_name_.c_str());
        }
    }
    /**
     * @brief Return the boost shared memory manager.
     */
    auto get_managed_shared_memory_ptr() { return managed_shared_memory_.get(); }

    std::string shm_name(char const *prefix, std::size_t i)
    {
        std::stringstream ss;
        ss << prefix << "-" << i;
        return ss.str();
    }

    std::string shm_name(char const *prefix, std::size_t i, std::size_t j)
    {
        std::stringstream ss;
        ss << prefix << "-" << i << "-" << j;
        return ss.str();
    }

    std::string shm4_connection_name(std::size_t i)
    {
        std::stringstream ss;
        ss << database_name_ << "-" << param::connection << "-" << i;
        return ss.str();
    }
    std::string shm4_row_queue_name(std::size_t i)
    {
        std::stringstream ss;
        ss << database_name_ << "-" << param::resultset << "-" << i;
        return ss.str();
    }

    bool is_alive()
    {
        try {
            boost::interprocess::managed_shared_memory msm(boost::interprocess::open_only, database_name_.c_str());
            return true;
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            return false;
        }
    }

private:
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_;
    std::string database_name_;
    bool owner_;
};

};  // namespace ogawayama::common

#endif //  SHARED_MEMORY_H_
