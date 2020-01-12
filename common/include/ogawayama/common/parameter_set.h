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
#pragma once

#include "boost/lockfree/spsc_queue.hpp"
#include "boost/interprocess/sync/interprocess_condition.hpp"
#include "boost/interprocess/sync/interprocess_mutex.hpp"
#include "boost/bind.hpp"
#include "boost/interprocess/smart_ptr/shared_ptr.hpp"

#include "ogawayama/stub/metadata.h"
#include "ogawayama/common/shared_memory.h"

namespace ogawayama::common {

    /**
     * @brief one to one communication channel, intended for communication between server and stub through boost binary_archive.
     */
    class ParameterSet
    {
    public:
        /**
         * @brief core class in the communication channel, using boost::circular_buffer.
         */
        class Parameters {
        public:
            /**
             * @brief Construct a new object.
             */
            Parameters(VoidAllocator allocator) : m_container_(allocator) {}

            /**
             * @brief Copy and move constructers are deleted.
             */
            Parameters(Parameters const&) = delete;
            Parameters(Parameters&&) = delete;
            Parameters& operator = (Parameters const&) = delete;
            Parameters& operator = (Parameters&&) = delete;

            ShmRowArgs& get_params() {
                return m_container_;
            }
            void clear() {
                m_container_.clear();
            }
        private:
            ShmRowArgs m_container_;
        };
        
    public:
        /**
         * @brief Construct a new object.
         */
        ParameterSet(std::string_view name, SharedMemory *shared_memory, bool owner) : shared_memory_(shared_memory), owner_(owner)
        {
            strncpy(name_, name.data(), name.length());
            auto mem = shared_memory_->get_managed_shared_memory_ptr();
            if (owner_) {
                mem->destroy<Parameters>(name_);
                params_ = mem->construct<Parameters>(name_)(mem->get_segment_manager());
            } else {
                params_ = mem->find<Parameters>(name_).first;
                if (params_ == nullptr) {
                    throw SharedMemoryException("can't find shared memory for ParameterSet");
                }
            }
        }
        ParameterSet(std::string_view name, SharedMemory *shared_memory) : ParameterSet(name, shared_memory, false) {}

        /**
         * @brief Destruct this object.
         */
        ~ParameterSet()
        {
            if (owner_) {
                shared_memory_->get_managed_shared_memory_ptr()->destroy<Parameters>(name_);
            }
        }

        template<typename T>
        void set_parameter(T value) { ShmClmArg arg = value; params_->get_params().emplace_back(arg); }
        void set_parameter(std::string_view value) {
            ShmString arg_string(shared_memory_->get_managed_shared_memory_ptr()->get_segment_manager());
            arg_string.assign(value.begin(), value.end());
            ShmClmArg arg = arg_string;
            params_->get_params().emplace_back(arg);
        }

        /**
         * @brief return the parameters in std::vector.
         */
        ShmRowArgs& get_params() {
            return params_->get_params();
        }

    private:
        SharedMemory *shared_memory_;
        Parameters *params_;
        const bool owner_;
        char name_[param::MAX_NAME_LENGTH]{};
    };
    
};  // namespace ogawayama::common
