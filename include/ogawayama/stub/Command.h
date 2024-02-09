/*
 * Copyright 2019-2023 Project Tsurugi.
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
 *
 *	@file	Command.h
 *	@brief  the command class dipatched to ogawayama
 */
#pragma once

class Command {
    public:
        // C'tors
        Command(std::string command_type_name, uint64_t object_id)
            : command_type_name(std::move(command_type_name)), object_id(object_id) {};
    std::string get_command_type_name() { return command_type_name; }
    [[nodiscard]] uint64_t get_object_id() const { return object_id; }

    private:
        std::string command_type_name; //command type name ex)"CREATE TABLE"
        uint64_t object_id;             // id of table meta data object
};
