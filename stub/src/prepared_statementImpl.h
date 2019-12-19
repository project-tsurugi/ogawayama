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

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/common/parameter_set.h"
#include "transactionImpl.h"
#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of PreparedStatement::Impl class
 */
class PreparedStatement::Impl
{
public:
    Impl(PreparedStatement *prepared_statement, std::size_t sid, ogawayama::common::ParameterSet* parameters)
        : envelope_(prepared_statement), sid_(sid), parameters_(parameters) {}
    ~Impl() = default;

    auto get_sid() { return sid_; }
    void clear() { parameters_->clear(); }
    template<typename T>
    void set_parameter(T param) {
        parameters_->set_parameter(param);
    }

private:
    PreparedStatement *envelope_;

    ogawayama::common::ParameterSet* parameters_;
    std::size_t sid_;
};

}  // namespace ogawayama::stub
